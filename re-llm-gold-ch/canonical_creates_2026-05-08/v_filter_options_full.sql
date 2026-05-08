-- gold_ch.v_filter_options_full
CREATE VIEW gold_ch.v_filter_options_full AS
 SELECT core_filter_options.product,
    core_filter_options.domain,
    core_filter_options.filter_type,
    core_filter_options.filter_value,
    core_filter_options.filter_label,
    core_filter_options.parent_value,
    core_filter_options.sort_order,
    core_filter_options.count,
    core_filter_options.is_active
   FROM gold_ch.core_filter_options
UNION ALL
 SELECT static_filter_options.product,
    static_filter_options.domain,
    static_filter_options.filter_type,
    static_filter_options.filter_value,
    static_filter_options.filter_label,
    static_filter_options.parent_value,
    static_filter_options.sort_order,
    static_filter_options.count,
    static_filter_options.is_active
   FROM gold_ch.static_filter_options
