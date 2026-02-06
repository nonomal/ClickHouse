#pragma once
#include <functional>
#include <Core/Types.h>
#include <Storages/ObjectStorage/DataLakes/Common/FileListCache.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;
std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix,
    std::optional<DataLakeFileListCachePtr> cache = {},
    bool bypass_cache = false);
/// TODO: ^^^ make better passing rather than by optional value
/// also avoid default param here (double check) to pass/skip caching explicitly
/// TODO: bypass_cache TBD as part of Context - the setting which may come from query, session, config

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need,
    std::optional<DataLakeFileListCachePtr> cache = {},
    bool bypass_cache = false);
}
