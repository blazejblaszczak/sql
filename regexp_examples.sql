-- getting stock ticker from text

WITH ex AS (
    SELECT 'Not sure it is a trusted source, by $AAPL seemingly edging closer to buying $MANU. Test $U, and maybe $IBM as well.' AS txt
)
SELECT *
, TRIM((regexp_matches(txt, '\$\w{1,4}', 'g'))[1], '$') AS stock_ticker
FROM ex


-- filtering dataset to get only rows with digits in particular column

WITH ex AS ( -- adding digits and strings columns for filtering in final query
    SELECT *
    , regexp_matches(feature_flag_value, '[0-9]+') AS digits
    , regexp_matches(feature_flag_value, '[a-z]+') AS strings
    FROM feature_flags
    WHERE TRUE
    AND feature_flag_type LIKE 'AB_%'
)
SELECT *
FROM ex
WHERE TRUE
AND strings IS NULL
