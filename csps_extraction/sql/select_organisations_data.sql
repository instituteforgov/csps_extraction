-- Replicates the collated data for the CSPS organisations data working file
-- NB: This turns all dates into 'periods', to facilitate temporal joins. These are defined as year * 4 + quarter, so e.g. 2020 Q4 becomes 2020 * 4 + 4 = 8084, with nulls set to 0 for start_period and the maximum integer that can be held in a SQL int column for end_period
-- NB: survey_period in the CSPS data is set to year * 4 + 4, because all CSPS data is from quarter 4 of each year
-- NB: Temporal joins use _between_, which includes both endpoints, because start/end year/quarters in civil_service.organisation are inclusive and non-overlapping. I.e. if an organisation ends in period N, it's successor starts in period N + 1
-- NB: Join between `civil_service.organisation` and `civil_service.vw_organisation_departmental_group` needs to be a left join as organisation aggregations and disaggregations don't feature in `civil_service.vw_organisation_departmental_group`, by design
-- NB: `case` statements do two things:
    -- 1. `Organisation` column: Add ' - <yyyy> iteration' strings that were cleaned as part of the extraction and loading of the data into the database back in to organisation names, to facilitate comparison between the collated data generated using this script and that in the original working file
    -- 2. (Specific to the CSPS organisations data) `Departmental group`, `Latest organisation`, `Latest IfG departmental group` columns: Handle organisations with type 'Aggregation' or 'Disaggregation' that feature in the source data, as these don't feature in civil_service.vw_organisation_departmental_group and civil_service.vw_organisation_latest
-- NB: 'Latest IfG departmental group' is renamed 'Latest departmental group', so that existing PivotTables connections to collated datasets don't break
with cspso as (
    select
        *,
        year * 4 + 4 survey_period
    from civil_service.civil_service_people_survey_organisations
),
o_vicd_vodg as (
    select
        o.id,
        vodg.organisation_name,
        o.type,
        vicd.is_ifg_core_department,
        vodg.ifg_departmental_group_id,
        vodg.ifg_departmental_group_name,
        vodg.ifg_departmental_group_short_name,
        vodg.start_year,
        vodg.start_quarter,
        vodg.end_year,
        vodg.end_quarter,
        isnull(vodg.start_year * 4 + vodg.start_quarter, 0) start_period,
        isnull(vodg.end_year * 4 + vodg.end_quarter, 2147483647) end_period
    from civil_service.organisation o
        left join civil_service.vw_ifg_core_departments vicd on
            o.id = vicd.organisation_id
        left join civil_service.vw_organisation_departmental_group vodg on
            o.id = vodg.organisation_id
)
select
    cspso.id,
    cspso.headline_category [Headline category],
    cspso.year [Year],
    case
        when cspso.organisation_name = 'Department for Culture, Media and Sport' and o_vicd_vodg.end_year = 2017 then 'Department for Culture, Media and Sport - 2017 iteration'
        when cspso.organisation_name = 'Department for Culture, Media and Sport' and o_vicd_vodg.start_year = 2023 then 'Department for Culture, Media and Sport - 2023 iteration'
        when cspso.organisation_name = 'Ministry of Housing, Communities & Local Government' and o_vicd_vodg.start_year = 2018 then 'Ministry of Housing, Communities & Local Government - 2018 iteration'
        when cspso.organisation_name = 'Ministry of Housing, Communities & Local Government' and o_vicd_vodg.start_year = 2024 then 'Ministry of Housing, Communities & Local Government - 2024 iteration'
        else cspso.organisation_name
    end [Organisation],
    case
        when o_vicd_vodg.type in ('Aggregation', 'Disaggregation', 'Reporting total') then 'Y'
        else null
    end [Organisation aggregation?],
    case cspso.organisation_name
        when 'All employees' then 'All employees'
        when 'Cabinet Office group (including agencies)' then 'CO'
        when 'Civil Service benchmark' then 'Civil Service benchmark'
        when 'Department for Education group (including agencies)' then 'DfE'
        when 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service' then 'DWP'
        when 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland' then 'Scot Gov'
        when 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)' then 'MoJ'
        when 'Ministry of Justice arm''s length bodies' then 'MoJ'
        when 'Ministry of Justice group (including agencies)' then 'MoJ'
        when 'National Offender Management Service group (including agencies)' then 'MoJ'
        when 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland' then 'Various'
        when 'UK Statistics Authority (excluding Office for National Statistics)' then 'CO'
        else o_vicd_vodg.ifg_departmental_group_short_name
    end [Departmental group],
    case
        when o_vicd_vodg.type in ('Aggregation', 'Disaggregation', 'Reporting total') then 'Combination'
        else o_vicd_vodg.type
    end [Organisation type],
    o_vicd_vodg.is_ifg_core_department [IfG core department],
    case cspso.organisation_name
        when 'All employees' then 'All employees'
        when 'Cabinet Office group (including agencies)' then 'Cabinet Office group (including agencies)'
        when 'Civil Service benchmark' then 'Civil Service benchmark'
        when 'Department for Education group (including agencies)' then 'Department for Education group (including agencies)'
        when 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service' then 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service'
        when 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland' then 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland'
        when 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)' then 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)'
        when 'Ministry of Justice arm''s length bodies' then 'Ministry of Justice arm''s length bodies'
        when 'Ministry of Justice group (including agencies)' then 'Ministry of Justice group (including agencies)'
        when 'National Offender Management Service group (including agencies)' then 'National Offender Management Service group (including agencies)'
        when 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland' then 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland'
        when 'UK Statistics Authority (excluding Office for National Statistics)' then 'UK Statistics Authority (excluding Office for National Statistics)'
        else vol1.latest_organisation_name
    end [Latest organisation],
    case cspso.organisation_name
        when 'All employees' then 'All employees'
        when 'Cabinet Office group (including agencies)' then 'CO'
        when 'Civil Service benchmark' then 'Civil Service benchmark'
        when 'Department for Education group (including agencies)' then 'DfE'
        when 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service' then 'DWP'
        when 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland' then 'Scot Gov'
        when 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)' then 'MoJ'
        when 'Ministry of Justice arm''s length bodies' then 'MoJ'
        when 'Ministry of Justice group (including agencies)' then 'MoJ'
        when 'National Offender Management Service group (including agencies)' then 'MoJ'
        when 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland' then 'Various'
        when 'UK Statistics Authority (excluding Office for National Statistics)' then 'CO'
        else iif(
            vol2.latest_organisation_name = 'Indeterminate',
            vol2.latest_determinate_organisation_short_name,
            vol2.latest_organisation_short_name
        )
    end [Latest departmental group],
    cspso.section [Section],
    cspso.measure [Measure],
    cspso.label [Label],
    cspso.value [Value],
    cspso.answer_format [Answer format],
    cspso.based_on [Based on],
    cspso.notes [Notes]
from cspso
    left join o_vicd_vodg on
        cspso.organisation_id = o_vicd_vodg.id and
        cspso.survey_period between o_vicd_vodg.start_period and o_vicd_vodg.end_period
    left join civil_service.vw_organisation_latest vol1 on
        o_vicd_vodg.id = vol1.organisation_id
    left join civil_service.vw_organisation_latest vol2 on
        o_vicd_vodg.ifg_departmental_group_id = vol2.organisation_id
