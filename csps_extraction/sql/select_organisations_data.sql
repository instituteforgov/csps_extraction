-- NB: case statements do two things:
    -- 1. Add ' - <yyyy> iteration' strings that were cleaned as part of the extraction and loading of the data into the database back in to organisation names, to facilitate comparison between the collated data generated using this script and that in the original working file
    -- 2. (Specific to the CSPS organisations data) Handle organisations with type 'Aggregation' or 'Disaggregation' that feature in the source data, as these don't feature in civil_service.vw_organisation_departmental_group and civil_service.vw_organisation_latest
-- NB: Temporal joins treat CSPS data as quarter 4 of the year. start/end year/quarters in civil_service.organisation are inclusive and non-overlapping, and either bound may be null (meaning no bound in that direction).
select
    cspso.id,
    cspso.headline_category [Headline category],
    cspso.year [Year],
    case
        when cspso.organisation_name = 'Department for Culture, Media and Sport' and o.end_year = 2017 then 'Department for Culture, Media and Sport - 2017 iteration'
        when cspso.organisation_name = 'Department for Culture, Media and Sport' and o.start_year = 2024 then 'Department for Culture, Media and Sport - 2024 iteration'
        when cspso.organisation_name = 'Ministry of Housing, Communities & Local Government' and o.start_year = 2018 then 'Ministry of Housing, Communities & Local Government - 2018 iteration'
        when cspso.organisation_name = 'Ministry of Housing, Communities & Local Government' and o.start_year = 2024 then 'Ministry of Housing, Communities & Local Government - 2024 iteration'
        else cspso.organisation_name
    end [Organisation],
    case
        when o.type in ('Aggregation', 'Disaggregation') then 'Y'
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
        else vodg.departmental_group_short_name
    end [Departmental group],
    case
        when o.type in ('Aggregation', 'Disaggregation') then 'Combination'
        else o.type
    end [Organisation type],
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
        else vol.latest_organisation_name
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
            vol.latest_organisation_name = 'Indeterminate',
            vol.latest_determinate_organisation_short_name,
            vol.latest_organisation_short_name
        )
    end [Latest IfG departmental group],
    cspso.section [Section],
    cspso.measure [Measure],
    cspso.label [Label],
    cspso.value [Value],
    cspso.answer_format [Answer format],
    cspso.based_on [Based on],
    cspso.notes [Notes]
from civil_service.civil_service_people_survey_organisations cspso
    left join civil_service.organisation o on
        cspso.organisation_id = o.id and
        (o.start_year is null or (cspso.year * 4 + 4) >= (o.start_year * 4 + o.start_quarter)) and
        (o.end_year is null or (cspso.year * 4 + 4) <= (o.end_year * 4 + o.end_quarter))
    left join civil_service.vw_organisation_departmental_group vodg on
        o.id = vodg.organisation_id and
        (vodg.start_year is null or (cspso.year * 4 + 4) >= (vodg.start_year * 4 + vodg.start_quarter)) and
        (vodg.end_year is null or (cspso.year * 4 + 4) <= (vodg.end_year * 4 + vodg.end_quarter))
    left join civil_service.vw_organisation_latest vol on
        vodg.ifg_departmental_group_id = vol.organisation_id
order by
    cspso.year,
    cspso.organisation_name
