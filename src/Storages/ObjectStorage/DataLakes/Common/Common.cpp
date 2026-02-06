#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

namespace DB
{

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix,
    std::optional<DataLakeFileListCachePtr> cache,
    bool bypass_cache)
{
    return listFiles(
        object_storage,
        path,
        prefix,
        [&suffix](const RelativePathWithMetadata & files_with_metadata) { return files_with_metadata.relative_path.ends_with(suffix); },
        cache,
        bypass_cache);
}


std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need,
    std::optional<DataLakeFileListCachePtr> cache,
    bool bypass_cache)
{
    /// TODO: are we sure this is a universal x-data-lake path modifier? double check for DeltaLake
    auto key = std::filesystem::path(path) / prefix;

    auto create_fn = [&]()
    {
        RelativePathsWithMetadata files_with_metadata;
        object_storage.listObjects(key, files_with_metadata, 0);
        Strings res;
        for (const auto & file_with_metadata : files_with_metadata)
        {
            if (check_need(*file_with_metadata))
                res.push_back(file_with_metadata->relative_path);
        }
        LOG_TRACE(getLogger("DataLakeCommon"), "Listed {} files ({})", res.size(), fmt::join(res, ", "));
        return res;
    };

    if (cache)
    {
        if (bypass_cache)
            return cache.value()->simulateGetAndSetLatest(key, create_fn);
        else
            return cache.value()->simulateGetTolerated(key, create_fn, 3);
    }
    return create_fn();
}

}
