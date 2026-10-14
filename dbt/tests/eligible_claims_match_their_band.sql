-- An eligible claim pays the band amount, or half of it when rerouting reduced it.
select claim_id, distance_band, amount_eur, is_reduced
from {{ ref('fct_eu261_claim') }}
where is_eligible
  and amount_eur != (
      case distance_band when 'short' then 250 when 'medium' then 400 else 600 end
      * case when is_reduced then 0.5 else 1 end
  )
