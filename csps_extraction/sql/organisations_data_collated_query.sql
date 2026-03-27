select
    cspso.id,
    cspso.headline_category [Headline category],
    cspso.year [Year],
    cspso.organisation_code [Organisation code],
    cspso.organisation [Organisation],
    cspso.departmental_group_survey [Departmental group (survey)],
    case
        when o.organisation is null then 'Y'
        else null
    end [Organisation aggregation?],
    case cspso.organisation
        when 'All employees' then 'All employees'
        when 'Civil Service benchmark' then 'Civil Service benchmark'
        when 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service' then 'DWP'
        when 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland' then 'Various'
        when 'Ministry of Justice group (including agencies)' then 'MoJ'
        when 'Ministry of Justice arm''s length bodies' then 'MoJ'
        when 'National Offender Management Service group (including agencies)' then 'MoJ'
        when 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland' then 'Scot Gov'
        when 'UK Statistics Authority (excluding Office for National Statistics)' then 'CO'
        when 'Department for Education group (including agencies)' then 'DfE'
        when 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)' then 'MoJ'
        when 'Cabinet Office group (including agencies)' then 'CO'
        else o.departmental_group
    end [Departmental group],
    case
        when o.organisation is null then 'Combination'
        else o.organisation_type
    end [Organisation type],
    case cspso.organisation
        when 'All employees' then 'All employees'
        when 'Civil Service benchmark' then 'Civil Service benchmark'
        when 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service' then 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service'
        when 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland' then 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland'
        when 'Ministry of Justice group (including agencies)' then 'Ministry of Justice group (including agencies)'
        when 'Ministry of Justice arm''s length bodies' then 'Ministry of Justice arm''s length bodies'
        when 'National Offender Management Service group (including agencies)' then 'National Offender Management Service group (including agencies)'
        when 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland' then 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland'
        when 'UK Statistics Authority (excluding Office for National Statistics)' then 'UK Statistics Authority (excluding Office for National Statistics)'
        when 'Department for Education group (including agencies)' then 'Department for Education group (including agencies)'
        when 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)' then 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)'
        when 'Cabinet Office group (including agencies)' then 'Cabinet Office group (including agencies)'
        else lo.latest_organisation
    end [Latest organisation],
    case cspso.organisation
        when 'All employees' then 'All employees'
        when 'Civil Service benchmark' then 'Civil Service benchmark'
        when 'Department for Work and Pensions, Jobcentre Plus and Pensions & Disability Carers Service' then 'DWP'
        when 'Scotland, Wales and Northern Ireland Offices, and the Office of the Advocate General for Scotland' then 'Various'
        when 'Ministry of Justice group (including agencies)' then 'MoJ'
        when 'Ministry of Justice arm''s length bodies' then 'MoJ'
        when 'National Offender Management Service group (including agencies)' then 'MoJ'
        when 'Historic Scotland and the Royal Commission on the Ancient and Historic Monuments of Scotland' then 'Scot Gov'
        when 'UK Statistics Authority (excluding Office for National Statistics)' then 'CO'
        when 'Department for Education group (including agencies)' then 'DfE'
        when 'HM Prison and Probation Service (excluding HM Prison Service and National Probation Service/Probation Service)' then 'MoJ'
        when 'Cabinet Office group (including agencies)' then 'CO'
        else lo.latest_departmental_group
    end [Latest departmental group],
    cspso.section [Section],
    cspso.measure [Measure],
    cspso.label [Label],
    cspso.value [Value],
    cspso.answer_format [Answer format],
    cspso.based_on [Based on],
    cspso.notes [Notes]
from civil_service.civil_service_people_survey_organisations cspso
    inner join civil_service.release_number rn on
        cspso.year = rn.year and
        rn.quarter = 4
    left join civil_service.organisation o on
        cspso.organisation_id = o.id
    left join civil_service.vw_latest_organisation lo on
        o.id = lo.organisation_id
