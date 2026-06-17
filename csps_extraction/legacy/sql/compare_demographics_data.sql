-- Replicates the collated data for the CSPS demographics working file
select
    cspsd.id ID,
    cspsd.headline_category [Headline category],
    cspsd.year [Year],
    cspsd.demographic_variable [Demographic variable],
    cspsd.response [Response],
    cspsd.section [Section],
    cspsd.measure [Measure],
    cspsd.label [Label],
    cspsd.count [Count],
    cspsd.value [Value],
    cspsd.answer_format [Answer format],
    cspsd.based_on [Based on],
    cspsd.notes [Notes]
from civil_service.civil_service_people_survey_demographics cspsd
