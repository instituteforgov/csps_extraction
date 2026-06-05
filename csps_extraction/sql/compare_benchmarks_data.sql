-- Replicates the collated data for the CSPS benchmarks working file
select
    cspsb.id,
    cspsb.headline_category [Headline category],
    cspsb.year [Year],
    cspsb.section [Section],
    cspsb.measure [Measure],
    cspsb.label [Label],
    cspsb.value [Value],
    cspsb.answer_format [Answer format],
    cspsb.based_on [Based on],
    cspsb.notes [Notes]
from civil_service.civil_service_people_survey_benchmarks cspsb
