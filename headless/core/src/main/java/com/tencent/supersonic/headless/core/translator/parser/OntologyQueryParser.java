package com.tencent.supersonic.headless.core.translator.parser;

import com.alibaba.fastjson.JSONObject;
import com.tencent.supersonic.headless.core.pojo.Ontology;
import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import com.tencent.supersonic.headless.core.translator.parser.calcite.RuntimeOptions;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.SqlBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Objects;

/** the calcite parse implements */
@Component("OntologyQueryParser")
@Slf4j
public class OntologyQueryParser implements QueryParser {

    @Override
    public boolean accept(QueryStatement queryStatement) {
        return Objects.nonNull(queryStatement.getOntologyQuery());
    }

    @Override
    public void parse(QueryStatement queryStatement) throws Exception {
        Ontology ontology = queryStatement.getOntology();
        S2CalciteSchema semanticSchema = S2CalciteSchema.builder()
                .schemaKey("DATASET_" + queryStatement.getDataSetId()).ontology(ontology)
                .runtimeOptions(RuntimeOptions.builder().minMaxTime(queryStatement.getMinMaxTime())
                        .enableOptimize(queryStatement.getEnableOptimize()).build())
                .build();
        SqlBuilder sqlBuilder = new SqlBuilder(semanticSchema);
        String sql = sqlBuilder.buildOntologySql(queryStatement);
        System.out.println(sql);
        queryStatement.setSql(sql);
    }

    public static void main(String[] args) throws Exception {
        QueryStatement statement = JSONObject.parseObject(JSON, QueryStatement.class);
        new OntologyQueryParser().parse(statement);
    }

    private static final String JSON = """
            {
                "dataSetId": 129,
                "enableOptimize": true,
                "isS2SQL": true,
                "isTranslated": false,
                "limit": 1000,
                "ok": true,
                "ontology": {
                    "dataModelMap": {
                        "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf": {
                            "aggTime": "none",
                            "dimensions": [
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                    "dataType": "UNKNOWN",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName"
                                },
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                    "dataType": "UNKNOWN",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname"
                                },
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                    "dataType": "UNKNOWN",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex"
                                },
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                    "dataType": "UNKNOWN",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code"
                                }
                            ],
                            "id": 291,
                            "identifiers": [],
                            "measures": [
                                {
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID"
                                },
                                {
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age"
                                },
                                {
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major"
                                },
                                {
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                    "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor"
                                }
                            ],
                            "modelId": 39,
                            "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "tableQuery": "__mdv_table_SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf__",
                            "type": "mysql"
                        },
                        "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5": {
                            "aggTime": "none",
                            "dimensions": [],
                            "id": 293,
                            "identifiers": [],
                            "measures": [
                                {
                                    "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                    "name": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID"
                                },
                                {
                                    "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                    "name": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID"
                                }
                            ],
                            "modelId": 39,
                            "name": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5",
                            "tableQuery": "__mdv_table_SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5__",
                            "type": "mysql"
                        },
                        "SCHEMA_a88462ebd1444c9fb4141d520e85a853": {
                            "aggTime": "none",
                            "dimensions": [
                                {
                                    "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                    "dataType": "UNKNOWN",
                                    "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                    "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType"
                                }
                            ],
                            "id": 292,
                            "identifiers": [],
                            "measures": [
                                {
                                    "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                    "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID"
                                },
                                {
                                    "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                    "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age"
                                },
                                {
                                    "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                    "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight"
                                }
                            ],
                            "modelId": 39,
                            "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853",
                            "tableQuery": "__mdv_table_SCHEMA_a88462ebd1444c9fb4141d520e85a853__",
                            "type": "mysql"
                        }
                    },
                    "database": {
                        "admins": [],
                        "createdAt": 1734576843000,
                        "createdBy": "admin",
                        "description": "MDV_数据源",
                        "hasEditPermission": false,
                        "hasPermission": false,
                        "hasUsePermission": false,
                        "host": "",
                        "id": 39,
                        "name": "MDV_DATASOURCE",
                        "password": "6LsrPFxrlbUEWEoVGdVyyw==",
                        "port": "",
                        "type": "mysql",
                        "updatedAt": 1734576843000,
                        "updatedBy": "admin",
                        "url": "",
                        "username": "",
                        "viewers": []
                    },
                    "databaseType": "MYSQL",
                    "dimensionMap": {
                        "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf": [
                            {
                                "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                "dataType": "UNKNOWN",
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                "ext": {},
                                "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                "owners": "admin"
                            },
                            {
                                "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                "dataType": "UNKNOWN",
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                "ext": {},
                                "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                "owners": "admin"
                            },
                            {
                                "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                "dataType": "UNKNOWN",
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                "ext": {},
                                "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                "owners": "admin"
                            },
                            {
                                "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                "dataType": "UNKNOWN",
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                "ext": {},
                                "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                "owners": "admin"
                            }
                        ],
                        "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5": [],
                        "SCHEMA_a88462ebd1444c9fb4141d520e85a853": [
                            {
                                "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                "dataType": "UNKNOWN",
                                "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                "ext": {},
                                "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                "owners": "admin"
                            }
                        ]
                    },
                    "dimensions": [
                        {
                            "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[0]"
                        },
                        {
                            "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[1]"
                        },
                        {
                            "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[2]"
                        },
                        {
                            "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[3]"
                        },
                        {
                            "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_a88462ebd1444c9fb4141d520e85a853[0]"
                        }
                    ],
                    "joinRelations": [
                        {
                            "id": 3619,
                            "joinCondition": [
                                {
                                    "left": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                    "middle": "=",
                                    "right": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID"
                                }
                            ],
                            "joinType": "join",
                            "left": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "right": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5"
                        },
                        {
                            "id": 3620,
                            "joinCondition": [
                                {
                                    "left": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                    "middle": "=",
                                    "right": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID"
                                }
                            ],
                            "joinType": "join",
                            "left": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5",
                            "right": "SCHEMA_a88462ebd1444c9fb4141d520e85a853"
                        }
                    ],
                    "materializationList": [],
                    "metrics": [
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID"
                                    }
                                ]
                            },
                            "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age"
                                    }
                                ]
                            },
                            "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major"
                                    }
                                ]
                            },
                            "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor"
                                    }
                                ]
                            },
                            "name": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID"
                                    }
                                ]
                            },
                            "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age"
                                    }
                                ]
                            },
                            "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight"
                                    }
                                ]
                            },
                            "name": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID"
                                    }
                                ]
                            },
                            "name": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        },
                        {
                            "metricTypeParams": {
                                "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                "fieldMetric": false,
                                "measures": [
                                    {
                                        "name": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID"
                                    }
                                ]
                            },
                            "name": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                            "owners": [
                                "admin"
                            ],
                            "type": "ATOMIC"
                        }
                    ]
                },
                "ontologyQuery": {
                    "aggOption": "NATIVE",
                    "dimensions": [],
                    "metrics": [
                        "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                        "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                        "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID"
                    ],
                    "nativeQuery": true
                },
                "semanticSchema": {
                    "dataSetId": 129,
                    "databaseResp": {
                        "$ref": "$.ontology.database"
                    },
                    "dimensions": [
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                            "createdAt": 1739715960000,
                            "createdBy": "admin",
                            "description": "",
                            "domainId": 7,
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                            "ext": {
                                "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[0].ext"
                            },
                            "fields": [],
                            "id": 742,
                            "isTag": 0,
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "LName",
                            "partitionTime": false,
                            "semanticType": "CATEGORY",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "timeDimension": false,
                            "type": "categorical",
                            "typeEnum": "DIMENSION",
                            "updatedAt": 1739715960000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                            "createdAt": 1739715960000,
                            "createdBy": "admin",
                            "description": "",
                            "domainId": 7,
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                            "ext": {
                                "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[1].ext"
                            },
                            "fields": [],
                            "id": 743,
                            "isTag": 0,
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "Fname",
                            "partitionTime": false,
                            "semanticType": "CATEGORY",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "timeDimension": false,
                            "type": "categorical",
                            "typeEnum": "DIMENSION",
                            "updatedAt": 1739715960000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                            "createdAt": 1739715960000,
                            "createdBy": "admin",
                            "description": "",
                            "domainId": 7,
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                            "ext": {
                                "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[2].ext"
                            },
                            "fields": [],
                            "id": 744,
                            "isTag": 0,
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "Sex",
                            "partitionTime": false,
                            "semanticType": "CATEGORY",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "timeDimension": false,
                            "type": "categorical",
                            "typeEnum": "DIMENSION",
                            "updatedAt": 1739715960000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                            "createdAt": 1739715960000,
                            "createdBy": "admin",
                            "description": "",
                            "domainId": 7,
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                            "ext": {
                                "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_5ea6ffddc1cd4a76941e92b0356a4fdf[3].ext"
                            },
                            "fields": [],
                            "id": 745,
                            "isTag": 0,
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "city_code",
                            "partitionTime": false,
                            "semanticType": "CATEGORY",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "timeDimension": false,
                            "type": "categorical",
                            "typeEnum": "DIMENSION",
                            "updatedAt": 1739715960000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                            "createdAt": 1739715979000,
                            "createdBy": "admin",
                            "description": "",
                            "domainId": 7,
                            "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                            "ext": {
                                "$ref": "$.ontology.dimensionMap.SCHEMA\\\\_a88462ebd1444c9fb4141d520e85a853[0].ext"
                            },
                            "fields": [],
                            "id": 746,
                            "isTag": 0,
                            "modelBizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853",
                            "modelId": 292,
                            "modelName": "pets",
                            "name": "PetType",
                            "partitionTime": false,
                            "semanticType": "CATEGORY",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "timeDimension": false,
                            "type": "categorical",
                            "typeEnum": "DIMENSION",
                            "updatedAt": 1739715979000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        }
                    ],
                    "metrics": [
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715961000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "StuID",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 649,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                        "isCreateMetric": 1,
                                        "name": "StuID"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "StuID",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715961000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715961000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "Age",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 650,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                        "isCreateMetric": 1,
                                        "name": "Age"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "Age",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715961000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715961000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "Major",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 651,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                        "isCreateMetric": 1,
                                        "name": "Major"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "Major",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715961000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715961000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "Advisor",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 652,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                        "isCreateMetric": 1,
                                        "name": "Advisor"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "modelId": 291,
                            "modelName": "student",
                            "name": "Advisor",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715961000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715979000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "PetID",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 653,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                        "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                        "isCreateMetric": 1,
                                        "name": "PetID"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853",
                            "modelId": 292,
                            "modelName": "pets",
                            "name": "PetID",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715979000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715979000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "pet_age",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 654,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                        "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                        "isCreateMetric": 1,
                                        "name": "pet_age"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853",
                            "modelId": 292,
                            "modelName": "pets",
                            "name": "pet_age",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715979000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715979000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "weight",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 655,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                        "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                        "isCreateMetric": 1,
                                        "name": "weight"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853",
                            "modelId": 292,
                            "modelName": "pets",
                            "name": "weight",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715979000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715987000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "StuID",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 656,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                        "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                        "fieldName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                        "isCreateMetric": 1,
                                        "name": "StuID"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5",
                            "modelId": 293,
                            "modelName": "has_pet",
                            "name": "StuID",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715987000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        },
                        {
                            "bizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                            "containsPartitionDimensions": false,
                            "createdAt": 1739715987000,
                            "createdBy": "admin",
                            "derived": false,
                            "description": "PetID",
                            "domainId": 7,
                            "drillDownDimensions": [],
                            "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                            "ext": {},
                            "fields": [],
                            "hasAdminRes": false,
                            "id": 657,
                            "isCollect": false,
                            "isPublish": 0,
                            "isTag": 0,
                            "metricDefineByMeasureParams": {
                                "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                "measures": [
                                    {
                                        "bizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                        "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                        "fieldName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                        "isCreateMetric": 1,
                                        "name": "PetID"
                                    }
                                ]
                            },
                            "metricDefineType": "MEASURE",
                            "modelBizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5",
                            "modelId": 293,
                            "modelName": "has_pet",
                            "name": "PetID",
                            "relaDimensionIdKey": "",
                            "sensitiveLevel": 0,
                            "similarity": 0.0,
                            "status": 1,
                            "type": "ATOMIC",
                            "typeEnum": "METRIC",
                            "updatedAt": 1739715987000,
                            "updatedBy": "admin",
                            "useCnt": 0
                        }
                    ],
                    "modelIds": [
                        291,
                        292,
                        293
                    ],
                    "modelRelas": [
                        {
                            "domainId": 7,
                            "fromModelId": 291,
                            "id": 3619,
                            "joinConditions": [
                                {
                                    "leftField": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                    "operator": "EQUALS",
                                    "rightField": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID"
                                }
                            ],
                            "joinType": "join",
                            "toModelId": 293
                        },
                        {
                            "domainId": 7,
                            "fromModelId": 293,
                            "id": 3620,
                            "joinConditions": [
                                {
                                    "leftField": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                    "operator": "EQUALS",
                                    "rightField": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID"
                                }
                            ],
                            "joinType": "join",
                            "toModelId": 292
                        }
                    ],
                    "modelResps": [
                        {
                            "adminOrgs": [],
                            "admins": [],
                            "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf",
                            "createdAt": 1739715960000,
                            "createdBy": "admin",
                            "databaseId": 39,
                            "domainId": 7,
                            "fieldList": [
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname"
                            ],
                            "id": 291,
                            "identifiers": [],
                            "measures": [
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                    "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID",
                                    "isCreateMetric": 1,
                                    "name": "StuID"
                                },
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                    "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age",
                                    "isCreateMetric": 1,
                                    "name": "Age"
                                },
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                    "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major",
                                    "isCreateMetric": 1,
                                    "name": "Major"
                                },
                                {
                                    "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                    "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                    "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor",
                                    "isCreateMetric": 1,
                                    "name": "Advisor"
                                }
                            ],
                            "modelDetail": {
                                "dimensions": [
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                        "dateFormat": "yyyy-MM-dd",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName",
                                        "isCreateDimension": 1,
                                        "name": "LName",
                                        "type": "categorical"
                                    },
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                        "dateFormat": "yyyy-MM-dd",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname",
                                        "isCreateDimension": 1,
                                        "name": "Fname",
                                        "type": "categorical"
                                    },
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                        "dateFormat": "yyyy-MM-dd",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex",
                                        "isCreateDimension": 1,
                                        "name": "Sex",
                                        "type": "categorical"
                                    },
                                    {
                                        "bizName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                        "dateFormat": "yyyy-MM-dd",
                                        "expr": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code",
                                        "isCreateDimension": 1,
                                        "name": "city_code",
                                        "type": "categorical"
                                    }
                                ],
                                "fields": [
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Sex"
                                    },
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_LName"
                                    },
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Fname"
                                    },
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_city_code"
                                    },
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age"
                                    },
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Major"
                                    },
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID"
                                    },
                                    {
                                        "fieldName": "SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Advisor"
                                    }
                                ],
                                "identifiers": [],
                                "measures": [
                                    {
                                        "$ref": "$.semanticSchema.modelResps[0].measures[0]"
                                    },
                                    {
                                        "$ref": "$.semanticSchema.modelResps[0].measures[1]"
                                    },
                                    {
                                        "$ref": "$.semanticSchema.modelResps[0].measures[2]"
                                    },
                                    {
                                        "$ref": "$.semanticSchema.modelResps[0].measures[3]"
                                    }
                                ],
                                "queryType": "table_query",
                                "sqlVariables": [],
                                "tableQuery": "__mdv_table_SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf__"
                            },
                            "name": "student",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "tagObjectId": 0,
                            "timeDimension": [],
                            "updatedAt": 1740128909000,
                            "updatedBy": "admin",
                            "viewOrgs": [],
                            "viewers": []
                        },
                        {
                            "adminOrgs": [],
                            "admins": [],
                            "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853",
                            "createdAt": 1739715979000,
                            "createdBy": "admin",
                            "databaseId": 39,
                            "domainId": 7,
                            "fieldList": [
                                "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight"
                            ],
                            "id": 292,
                            "identifiers": [],
                            "measures": [
                                {
                                    "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                    "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                    "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID",
                                    "isCreateMetric": 1,
                                    "name": "PetID"
                                },
                                {
                                    "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                    "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                    "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age",
                                    "isCreateMetric": 1,
                                    "name": "pet_age"
                                },
                                {
                                    "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                    "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                    "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight",
                                    "isCreateMetric": 1,
                                    "name": "weight"
                                }
                            ],
                            "modelDetail": {
                                "dimensions": [
                                    {
                                        "bizName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                        "dateFormat": "yyyy-MM-dd",
                                        "expr": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType",
                                        "isCreateDimension": 1,
                                        "name": "PetType",
                                        "type": "categorical"
                                    }
                                ],
                                "fields": [
                                    {
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetType"
                                    },
                                    {
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID"
                                    },
                                    {
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_weight"
                                    },
                                    {
                                        "fieldName": "SCHEMA_a88462ebd1444c9fb4141d520e85a853_pet_age"
                                    }
                                ],
                                "identifiers": [],
                                "measures": [
                                    {
                                        "$ref": "$.semanticSchema.modelResps[1].measures[0]"
                                    },
                                    {
                                        "$ref": "$.semanticSchema.modelResps[1].measures[1]"
                                    },
                                    {
                                        "$ref": "$.semanticSchema.modelResps[1].measures[2]"
                                    }
                                ],
                                "queryType": "table_query",
                                "sqlVariables": [],
                                "tableQuery": "__mdv_table_SCHEMA_a88462ebd1444c9fb4141d520e85a853__"
                            },
                            "name": "pets",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "tagObjectId": 0,
                            "timeDimension": [],
                            "updatedAt": 1740128917000,
                            "updatedBy": "admin",
                            "viewOrgs": [],
                            "viewers": []
                        },
                        {
                            "adminOrgs": [],
                            "admins": [],
                            "bizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5",
                            "createdAt": 1739715986000,
                            "createdBy": "admin",
                            "databaseId": 39,
                            "domainId": 7,
                            "fieldList": [
                                "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID"
                            ],
                            "id": 293,
                            "identifiers": [],
                            "measures": [
                                {
                                    "bizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                    "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                    "fieldName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID",
                                    "isCreateMetric": 1,
                                    "name": "StuID"
                                },
                                {
                                    "bizName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                    "expr": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                    "fieldName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID",
                                    "isCreateMetric": 1,
                                    "name": "PetID"
                                }
                            ],
                            "modelDetail": {
                                "dimensions": [],
                                "fields": [
                                    {
                                        "fieldName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_StuID"
                                    },
                                    {
                                        "fieldName": "SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5_PetID"
                                    }
                                ],
                                "identifiers": [],
                                "measures": [
                                    {
                                        "$ref": "$.semanticSchema.modelResps[2].measures[0]"
                                    },
                                    {
                                        "$ref": "$.semanticSchema.modelResps[2].measures[1]"
                                    }
                                ],
                                "queryType": "table_query",
                                "sqlVariables": [],
                                "tableQuery": "__mdv_table_SCHEMA_ae3aed14bfc04cd2a9c4f4210d8838b5__"
                            },
                            "name": "has_pet",
                            "sensitiveLevel": 0,
                            "status": 1,
                            "tagObjectId": 0,
                            "timeDimension": [],
                            "updatedAt": 1740128925000,
                            "updatedBy": "admin",
                            "viewOrgs": [],
                            "viewers": []
                        }
                    ],
                    "schemaType": "DATASET",
                    "tags": []
                },
                "sql": "SELECT SUM(SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID), COUNT(SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID) AS _宠物数量_ FROM t_129 GROUP BY SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID, SUM(SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID) HAVING SUM(SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age) > 20",
                "sqlQuery": {
                    "sql": "SELECT SUM(SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID), COUNT(SCHEMA_a88462ebd1444c9fb4141d520e85a853_PetID) AS _宠物数量_ FROM t_129 GROUP BY SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID, SUM(SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_StuID) HAVING SUM(SCHEMA_5ea6ffddc1cd4a76941e92b0356a4fdf_Age) > 20",
                    "supportWith": false,
                    "table": "t_129",
                    "withAlias": false
                },
                "status": 0,
                "translated": false
            }            """;

}
