-- The JOIN runtime filter settings and `rewrite_in_to_join` are in the Production tier
-- (they used to be Beta and Experimental). Assert the exact names and tiers, so a
-- regression that demotes some of them, or promotes only a part of the family, is caught.
-- Being Production also means they stay writable under `allow_feature_tier = 3`, which
-- makes a read-only constraint out of every non-production tier.

SELECT name, tier FROM system.settings
WHERE name IN (
    'enable_join_runtime_filters',
    'enable_join_runtime_filters_index_analysis',
    'join_runtime_bloom_filter_bytes',
    'join_runtime_bloom_filter_hash_functions',
    'join_runtime_bloom_filter_max_ratio_of_set_bits',
    'join_runtime_filter_blocks_to_skip_before_reenabling',
    'join_runtime_filter_exact_values_limit',
    'join_runtime_filter_from_fixed_hash_table',
    'join_runtime_filter_min_probe_rows',
    'join_runtime_filter_pass_ratio_threshold_for_disabling',
    'join_runtime_filter_size_from_hash_table_stats',
    'rewrite_in_to_join')
ORDER BY name;

-- None of them is left in a gated tier.
SELECT count() FROM system.settings
WHERE tier IN ('Beta', 'Experimental', 'PrivatePreview') AND name IN (
    'enable_join_runtime_filters',
    'enable_join_runtime_filters_index_analysis',
    'join_runtime_bloom_filter_bytes',
    'join_runtime_bloom_filter_hash_functions',
    'join_runtime_bloom_filter_max_ratio_of_set_bits',
    'join_runtime_filter_blocks_to_skip_before_reenabling',
    'join_runtime_filter_exact_values_limit',
    'join_runtime_filter_from_fixed_hash_table',
    'join_runtime_filter_min_probe_rows',
    'join_runtime_filter_pass_ratio_threshold_for_disabling',
    'join_runtime_filter_size_from_hash_table_stats',
    'rewrite_in_to_join');
