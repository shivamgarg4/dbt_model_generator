    {{ config(
        materialized='incremental',
        schema='DW',
        unique_key=["OPCO_CD"],
        merge_update_columns = ['OPCO_ID', 'DATA_SRC', 'UPDATE_BY', 'UPDATE_PGM', 'OPCO_DSC', 'RPT_OPCO_DSC', 'RPT_OPCO_ABBRV', 'TBA_ACTIVE_FLG']
    )}}
    
    -- Model: DW.D_OPCO
    -- Source: EBI_DEV_DB.LND_CORE.D_DP_OPCO
    
    SELECT
        '1' AS OPCO_ID,
        'DW.D_OPCO' AS DATA_SRC,
        CAST(CURRENT_USER() AS VARCHAR(200)) AS CREATE_BY,
        'DW.D_OPCO' AS CREATE_PGM,
        CAST(CURRENT_USER() AS VARCHAR(200)) AS UPDATE_BY,
        'DW.D_OPCO' AS UPDATE_PGM,
        'Y' AS TBA_ACTIVE_FLG,
        *
    FROM
    (
    SELECT
        OPCO_CD AS OPCO_CD,
        OPCO_DSC AS OPCO_DSC,
        RPT_OPCO_DSC AS RPT_OPCO_DSC,
        RPT_OPCO_ABBRV AS RPT_OPCO_ABBRV
    FROM {{ source('LND_CORE', 'D_DP_OPCO') }} AS source
    LEFT JOIN {{ ref('LND_CORE.D_ITEM') }} AS items ON source.OPCO_ID=items.OPCO_ID
    
    MINUS
    
    SELECT
        OPCO_CD AS OPCO_CD,
        OPCO_DSC AS OPCO_DSC,
        RPT_OPCO_DSC AS RPT_OPCO_DSC,
        RPT_OPCO_ABBRV AS RPT_OPCO_ABBRV
    FROM {{ source('DW','D_OPCO') }}
)