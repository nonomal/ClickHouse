#pragma once

#include <chrono>
#include <Common/CacheBase.h>
#include <Common/TTLCachePolicy.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace ProfileEvents
{
    extern const Event DataLakeFileListCacheMisses;
    extern const Event DataLakeFileListCacheExpiredMisses;
    extern const Event DataLakeFileListCacheHits;
    extern const Event DataLakeFileListCacheSets;
}

namespace CurrentMetrics
{
    extern const Metric DataLakeFileListCacheBytes;
    extern const Metric DataLakeFileListCachePaths;
}

namespace DB
{

struct DataLakeFileListCacheKey
{
    String path;
    std::optional<UUID> user_id;
    const std::chrono::time_point<std::chrono::system_clock> cached_at;
    const std::chrono::time_point<std::chrono::system_clock> expires_at;

    DataLakeFileListCacheKey(
        String path_,
        std::chrono::time_point<std::chrono::system_clock> cached_at_,
        std::chrono::time_point<std::chrono::system_clock> expires_at_)
        : path(path_)
        , cached_at(cached_at_)
        , expires_at(expires_at_)
    {}

    bool operator==(const DataLakeFileListCacheKey & other) const
    {
        return path == other.path;
    }

};

struct DataLakeFileListCacheEntry : private boost::noncopyable
{
    /// TODO: prevent list copying
    Strings file_list;

    explicit DataLakeFileListCacheEntry(Strings && file_list_)
        : file_list(std::move(file_list_))
    {
    }
};

struct DataLakeFileListCacheEntryHash
{
    size_t operator()(const DataLakeFileListCacheKey & key) const
    {
        return hash(key.path);
    }

    std::hash<std::string> hash;
};

struct DataLakeFileListCacheEntryWeight
{
    size_t operator()(const DataLakeFileListCacheEntry & entry) const
    {
        size_t sz = 0;
        for (const auto & file : entry.file_list)
        {
            sz += file.size();
        }
        return sz;
    }
};

struct DataLakeFileListCacheKeyIsStale
{
    bool operator()(const DataLakeFileListCacheKey & key) const
    {
        return std::chrono::system_clock::now() < key.expires_at;
    }
};


class DataLakeFileListCache : public CacheBase<DataLakeFileListCacheKey, DataLakeFileListCacheEntry, DataLakeFileListCacheEntryHash, DataLakeFileListCacheEntryWeight>
{
public:
    using Base = CacheBase<DataLakeFileListCacheKey, DataLakeFileListCacheEntry, DataLakeFileListCacheEntryHash, DataLakeFileListCacheEntryWeight>;

    explicit DataLakeFileListCache(size_t max_size_in_bytes)
        : Base(std::make_unique<TTLCachePolicy<DataLakeFileListCacheKey, DataLakeFileListCacheEntry, DataLakeFileListCacheEntryHash, DataLakeFileListCacheEntryWeight, DataLakeFileListCacheKeyIsStale>>(
            CurrentMetrics::DataLakeFileListCacheBytes, CurrentMetrics::DataLakeFileListCachePaths, std::make_unique<PerUserTTLCachePolicyUserQuota>()))
    {
        setMaxSizeInBytes(max_size_in_bytes);
        setMaxCount(999999);
    }

    template <typename LoadFunc>
    Strings simulateGetAndSetLatest(const String & path, LoadFunc && load_fn, bool update_metrics = true)
    {
        auto value = std::make_shared<DataLakeFileListCacheEntry>(std::move(load_fn()));
        DataLakeFileListCacheKey key(
            path,
            std::chrono::system_clock::now(),
            std::chrono::system_clock::now() + std::chrono::seconds(10)     /// TODO: this one if for TTL cleanup
         );
        Base::set(key, value);

        if (update_metrics)
            ProfileEvents::increment(ProfileEvents::DataLakeFileListCacheSets);

        return value->file_list;
    }

    template <typename LoadFunc>
    Strings simulateGetTolerated(const String & path, LoadFunc && load_fn, int64_t tolerated_staleness_in_seconds = 3) /// TODO: <<< deal with hardcode
    {
        DataLakeFileListCacheKey key(
            path,
            std::chrono::system_clock::now(),   /// TODO: this one is ignored in this context
            std::chrono::system_clock::now()    /// TODO: this one is ignored in this context
        );
        auto cached = Base::getWithKey(key);

        if (cached.has_value())
        {
            if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - cached.value().key.cached_at).count() <= tolerated_staleness_in_seconds)
            {
                ProfileEvents::increment(ProfileEvents::DataLakeFileListCacheHits);
                return cached.value().mapped->file_list;
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::DataLakeFileListCacheExpiredMisses);
            }
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::DataLakeFileListCacheMisses);
        }
        return simulateGetAndSetLatest(path, load_fn, false);
    }
};

using DataLakeFileListCachePtr = std::shared_ptr<DataLakeFileListCache>;

}
