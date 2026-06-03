-- Augments source data with IfG-derived organisation attributes
-- See README for an explanation of differences between the compare_organisations_data.sql script and this one
-- NB: 'IfG core department' is recoded to 'Y'/'N' to make it more user-friendly
-- NB: 'Organisation name' is renamed 'Organisation', so that existing PivotTable connections to collated datasets don't break
-- NB: 'Latest IfG departmental group' is renamed 'Latest departmental group', so that existing PivotTable connections to collated datasets don't break
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
    cspso.organisation_name [Organisation],
    o_vicd_vodg.type [Organisation type],
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
    iif(o_vicd_vodg.is_ifg_core_department = 1, 'Y', 'N') [IfG core department],
    case
        when o_vicd_vodg.type in ('Aggregation', 'Disaggregation') then cspso.organisation_name
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
        else vol2.latest_organisation_short_name
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
