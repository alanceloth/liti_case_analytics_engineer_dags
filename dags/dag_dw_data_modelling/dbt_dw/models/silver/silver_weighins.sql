WITH clean_data AS (
    SELECT
        _id AS wheiginId,
        CAST(REPLACE(CAST(age AS STRING), ',', '') AS INTEGER) AS age,
        CAST(REPLACE(CAST(bmi AS STRING), ',', '') AS DECIMAL) AS bmi,
        CAST(REPLACE(CAST(bmr AS STRING), ',', '') AS DECIMAL) AS bmr,
        CAST(REPLACE(CAST(smi AS STRING), ',', '') AS DECIMAL) AS smi,
        time AS time,
        state AS state,
        CAST(REPLACE(CAST(bfmMax AS STRING), ',', '') AS DECIMAL) AS bfmMax,
        CAST(REPLACE(CAST(bfmMin AS STRING), ',', '') AS DECIMAL) AS bfmMin,
        CAST(REPLACE(CAST(bfpMax AS STRING), ',', '') AS DECIMAL) AS bfpMax,
        CAST(REPLACE(CAST(bfpMin AS STRING), ',', '') AS DECIMAL) AS bfpMin,
        CAST(REPLACE(CAST(bmiMax AS STRING), ',', '') AS DECIMAL) AS bmiMax,
        CAST(REPLACE(CAST(bmiMin AS STRING), ',', '') AS DECIMAL) AS bmiMin,
        CAST(REPLACE(CAST(bmrMax AS STRING), ',', '') AS DECIMAL) AS bmrMax,
        CAST(REPLACE(CAST(bmrMin AS STRING), ',', '') AS DECIMAL) AS bmrMin,
        CAST(REPLACE(CAST(height AS STRING), ',', '') AS DECIMAL) AS height,
        CAST(REPLACE(CAST(smmMax AS STRING), ',', '') AS DECIMAL) AS smmMax,
        CAST(REPLACE(CAST(smmMin AS STRING), ',', '') AS DECIMAL) AS smmMin,
        CAST(REPLACE(CAST(allBody AS STRING), ',', '') AS DECIMAL) AS allBody,
        CAST(REPLACE(CAST(boneMax AS STRING), ',', '') AS DECIMAL) AS boneMax,
        CAST(REPLACE(CAST(boneMin AS STRING), ',', '') AS DECIMAL) AS boneMin,
        CAST(handled AS BOOL) AS handled,
        CAST(REPLACE(CAST(leftArm AS STRING), ',', '') AS DECIMAL) AS leftArm,
        CAST(REPLACE(CAST(leftLeg AS STRING), ',', '') AS DECIMAL) AS leftLeg,
        bodyType AS bodyType,
        CAST(REPLACE(CAST(boneMass AS STRING), ',', '') AS DECIMAL) AS boneMass,
        CAST(REPLACE(CAST(muscleKg AS STRING), ',', '') AS DECIMAL) AS muscleKg,
        nickName AS nickName,
        owner_id AS owner_id,
        CAST(REPLACE(CAST(rightArm AS STRING), ',', '') AS DECIMAL) AS rightArm,
        CAST(REPLACE(CAST(rightLeg AS STRING), ',', '') AS DECIMAL) AS rightLeg,
        timezone AS timezone,
        CAST(REPLACE(CAST(weightkg AS STRING), ',', '') AS DECIMAL) AS weightkg,
        CAST(REPLACE(CAST(weightlb AS STRING), ',', '') AS DECIMAL) AS weightlb,
        CAST(REPLACE(CAST(weightst AS STRING), ',', '') AS DECIMAL) AS weightst,
        CAST(REPLACE(CAST(allBodykg AS STRING), ',', '') AS DECIMAL) AS allBodykg,
        CAST(REPLACE(CAST(bodyScore AS STRING), ',', '') AS INTEGER) AS bodyScore,
        CASE
        WHEN SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) IS NOT NULL
                AND SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) <= CURRENT_DATETIME()
                AND SAFE_CAST(TIMESTAMP(createdAt) AS DATE) >= DATE '2021-05-13'
            THEN SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME)
            ELSE NULL
        END AS createdAt,
        CASE
        WHEN deletedAt != '' 
            AND SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME) IS NOT NULL
            AND SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME) <= CURRENT_DATETIME()
            AND SAFE_CAST(TIMESTAMP(deletedAt) AS DATE) >= DATE '2021-05-13'
        THEN SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME)
        ELSE NULL
        END AS deletedAt,
        electrode AS electrode,
        CAST(REPLACE(CAST(leftArmkg AS STRING), ',', '') AS DECIMAL) AS leftArmkg,
        CAST(REPLACE(CAST(leftLegkg AS STRING), ',', '') AS DECIMAL) AS leftLegkg,
        CAST(REPLACE(CAST(proteinKg AS STRING), ',', '') AS DECIMAL) AS proteinKg,
        CAST(REPLACE(CAST(smPercent AS STRING), ',', '') AS DECIMAL) AS smPercent,
        CAST(REPLACE(CAST(weightMax AS STRING), ',', '') AS DECIMAL) AS weightMax,
        CAST(REPLACE(CAST(weightMin AS STRING), ',', '') AS DECIMAL) AS weightMin,
        CAST(REPLACE(CAST(bfmControl AS STRING), ',', '') AS DECIMAL) AS bfmControl,
        CAST(REPLACE(CAST(ffmControl AS STRING), ',', '') AS DECIMAL) AS ffmControl,
        CAST(REPLACE(CAST(rightArmkg AS STRING), ',', '') AS DECIMAL) AS rightArmkg,
        CAST(REPLACE(CAST(rightLegkg AS STRING), ',', '') AS DECIMAL) AS rightLegkg,
        CAST(REPLACE(CAST(weightstlb AS STRING), ',', '') AS DECIMAL) AS weightstlb,
        CAST(REPLACE(CAST(bfmStandard AS STRING), ',', '') AS DECIMAL) AS bfmStandard,
        CAST(REPLACE(CAST(bfpStandard AS STRING), ',', '') AS DECIMAL) AS bfpStandard,
        CAST(REPLACE(CAST(bmiStandard AS STRING), ',', '') AS DECIMAL) AS bmiStandard,
        CAST(REPLACE(CAST(bmrStandard AS STRING), ',', '') AS DECIMAL) AS bmrStandard,
        CAST(REPLACE(CAST(bodyWaterKg AS STRING), ',', '') AS DECIMAL) AS bodyWaterKg,
        CAST(REPLACE(CAST(ffmStandard AS STRING), ',', '') AS DECIMAL) AS ffmStandard,
        isSupportHR AS isSupportHR,
        CAST(REPLACE(CAST(physicalAge AS STRING), ',', '') AS INTEGER) AS physicalAge,
        CAST(REPLACE(CAST(precisionKg AS STRING), ',', '') AS DECIMAL) AS precisionKg,
        CAST(REPLACE(CAST(precisionLb AS STRING), ',', '') AS DECIMAL) AS precisionLb,
        CAST(REPLACE(CAST(smmStandard AS STRING), ',', '') AS DECIMAL) AS smmStandard,
        CAST(REPLACE(CAST(temperature AS STRING), ',', '') AS DECIMAL) AS temperature,
        CAST(REPLACE(CAST(visceralFat AS STRING), ',', '') AS DECIMAL) AS visceralFat,
        CAST(REPLACE(CAST(weightGrama AS STRING), ',', '') AS DECIMAL) AS weightGrama,
        CAST(REPLACE(CAST(targetWeight AS STRING), ',', '') AS DECIMAL) AS targetWeight,
        CAST(REPLACE(CAST(waterMassMax AS STRING), ',', '') AS DECIMAL) AS waterMassMax,
        CAST(REPLACE(CAST(waterMassMin AS STRING), ',', '') AS DECIMAL) AS waterMassMin,
        CAST(REPLACE(CAST(allBodyMuscle AS STRING), ',', '') AS DECIMAL) AS allBodyMuscle,
        biologicalSex AS biologicalSex,
        bmiEvaluation AS bmiEvaluation,
        CAST(REPLACE(CAST(bodyFatMassKg AS STRING), ',', '') AS DECIMAL) AS bodyFatMassKg,
        CAST(REPLACE(CAST(leftArmmuscle AS STRING), ',', '') AS DECIMAL) AS leftArmmuscle,
        CAST(REPLACE(CAST(leftLegMuscle AS STRING), ',', '') AS DECIMAL) AS leftLegMuscle,
        CAST(REPLACE(CAST(muscleMassMax AS STRING), ',', '') AS DECIMAL) AS muscleMassMax,
        CAST(REPLACE(CAST(muscleMassMin AS STRING), ',', '') AS DECIMAL) AS muscleMassMin,
        CAST(REPLACE(CAST(musclePercent AS STRING), ',', '') AS DECIMAL) AS musclePercent,
        CAST(REPLACE(CAST(obesityDegree AS STRING), ',', '') AS DECIMAL) AS obesityDegree,
        CAST(REPLACE(CAST(precisionStlb AS STRING), ',', '') AS DECIMAL) AS precisionStlb,
        CAST(REPLACE(CAST(weightControl AS STRING), ',', '') AS DECIMAL) AS weightControl,
        CAST(REPLACE(CAST(bodyFatPercent AS STRING), ',', '') AS DECIMAL) AS bodyFatPercent,
        CAST(REPLACE(CAST(proteinMassMax AS STRING), ',', '') AS DECIMAL) AS proteinMassMax,
        CAST(REPLACE(CAST(proteinMassMin AS STRING), ',', '') AS DECIMAL) AS proteinMassMin,
        CAST(REPLACE(CAST(proteinPercent AS STRING), ',', '') AS DECIMAL) AS proteinPercent,
        CAST(REPLACE(CAST(rightArmMuscle AS STRING), ',', '') AS DECIMAL) AS rightArmMuscle,
        CAST(REPLACE(CAST(rightLegMuscle AS STRING), ',', '') AS DECIMAL) AS rightLegMuscle,
        CAST(REPLACE(CAST(weightStandard AS STRING), ',', '') AS DECIMAL) AS weightStandard,
        CAST(REPLACE(CAST(allBodyMusclekg AS STRING), ',', '') AS DECIMAL) AS allBodyMusclekg,
        CAST(REPLACE(CAST(kgScaleDivision AS STRING), ',', '') AS DECIMAL) AS kgScaleDivision,
        CAST(REPLACE(CAST(lbScaleDivision AS STRING), ',', '') AS DECIMAL) AS lbScaleDivision,
        CAST(REPLACE(CAST(leftArmMusclekg AS STRING), ',', '') AS DECIMAL) AS leftArmMusclekg,
        CAST(REPLACE(CAST(leftLegMusclekg AS STRING), ',', '') AS DECIMAL) AS leftLegMusclekg,
        CAST(REPLACE(CAST(moisturePercent AS STRING), ',', '') AS DECIMAL) AS moisturePercent,
        CAST(REPLACE(CAST(rightArmMusclekg AS STRING), ',', '') AS DECIMAL) AS rightArmMusclekg,
        CAST(REPLACE(CAST(rightLegMusclekg AS STRING), ',', '') AS DECIMAL) AS rightLegMusclekg,
        CAST(REPLACE(CAST(skeletalMuscleKg AS STRING), ',', '') AS DECIMAL) AS skeletalMuscleKg,
        bodyScoreEvaluation AS bodyScoreEvaluation,
        CAST(REPLACE(CAST(subcutaneousFatPercent AS STRING), ',', '') AS DECIMAL) AS subcutaneousFatPercent,
        muscleFatLeftArmEvaluation AS muscleFatLeftArmEvaluation,
        muscleFatLeftLegEvaluation AS muscleFatLeftLegEvaluation,
        muscleFatRightArmEvaluation AS muscleFatRightArmEvaluation,
        muscleFatRightLegEvaluation AS muscleFatRightLegEvaluation,
        muscleBalanceLeftArmEvaluation AS muscleBalanceLeftArmEvaluation,
        muscleBalanceLeftLegEvaluation AS muscleBalanceLeftLegEvaluation,
        muscleBalanceRightArmEvaluation AS muscleBalanceRightArmEvaluation,
        muscleBalanceRightLegEvaluation AS muscleBalanceRightLegEvaluation
    FROM {{ ref('bronze_weighins') }}
    WHERE SAFE_CAST(TIMESTAMP(createdAt) AS DATETIME) <= CURRENT_DATETIME()
    AND SAFE_CAST(TIMESTAMP(deletedAt) AS DATETIME) <= CURRENT_DATETIME()
)

SELECT * FROM clean_data

