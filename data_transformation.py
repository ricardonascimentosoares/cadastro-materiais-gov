import pyspark.sql.functions as F
from delta import *
from global_variables import *


def pdm_landing_to_bronze(spark):
    return (spark
            .read
            .format("csv")
            .option("header", True)
            .load(pdm_landing_path)
            .write
            .format("delta")
            .mode("overwrite")
            .save(pdm_bronze_path)
    )


def pdm_bronze_to_silver(spark):
    return (spark
            .read
            .format("delta")
            .load(pdm_bronze_path)
            .drop_duplicates(["Código"])
            .withColumn("codigoClasse", F.split('Classe', ':').getItem(0))
            .withColumn("descricaoClasse", F.split('Classe', ':').getItem(1))
            .withColumn("descricaoClasse", F.ltrim('descricaoClasse'))
            .drop('Classe')
            .withColumnRenamed("Código", "codigoPdm")
            .withColumnRenamed("Descrição", "descricaoPdm")
            .withColumn("descricaoPdm", F.trim('descricaoPdm'))
            .withColumn("descricaoClasse", F.trim('descricaoClasse'))     
            .withColumn("descricaoPdm", F.initcap("descricaoPdm"))   
            .withColumn("descricaoClasse", F.initcap("descricaoClasse"))       
            .write
            .format("delta")
            .mode("overwrite")
            .save(pdm_silver_path)
        )


def classes_landing_to_bronze(spark):
    return (spark
            .read
            .format("csv")
            .option("header", True)
            .load(classes_landing_path)
            .write
            .format("delta")
            .mode("overwrite")
            .save(classes_bronze_path)
    )

def classes_bronze_to_silver(spark):
    return (spark
            .read
            .format("delta")
            .load(classes_bronze_path)
            .withColumnRenamed("Código", "codigoClasse")
            .withColumnRenamed("Descricao", "descricaoClasse")
            .withColumn("codigoGrupo", F.split('Grupo', ':').getItem(0))
            .withColumn("descricaoGrupo", F.split('Grupo', ':').getItem(1))
            .withColumn("descricaoGrupo", F.trim('descricaoGrupo'))
            .withColumn("descricaoClasse", F.trim('descricaoClasse'))
            .withColumn("descricaoGrupo", F.initcap("descricaoGrupo"))   
            .withColumn("descricaoClasse", F.initcap("descricaoClasse"))     
            .drop("Grupo")
            .write
            .format("delta")
            .mode("overwrite")
            .save(classes_silver_path))


def grupos_landing_to_bronze(spark):
    return (spark
            .read
            .format("csv")
            .option("header", True)
            .load(grupos_landing_path)
            .write
            .format("delta")
            .mode("overwrite")
            .save(grupos_bronze_path))


def material_landing_to_bronze(spark):
    return (spark
            .read
            .format("json")
            .load(material_landing_path)
            .write
            .format("delta")
            .mode("overwrite")
            .save(material_bronze_path)
            )

def material_bronze_to_silver(spark):
    df_material = (spark
                   .read
                   .format("delta")
                   .load(material_bronze_path))
    df_classes = (spark
                  .read
                  .format('delta')
                  .load(classes_silver_path)
                  )
    return (df_material
            .join(F.broadcast(df_classes), ['codigoClasse'], "inner")
            .select('codigoItem', 
                    'codigoPdm', 
                    'nomePdm', 
                    'codigoClasse', 
                    'descricaoClasse', 
                    'codigoGrupo', 
                    'descricaoGrupo', 
                    'itemSuspenso', 
                    'itemSustentavel', 
                    'statusItem', 
                    'buscaItemCaracteristica')
            .write
            .format("delta")
            .mode("overwrite")
            .save(material_silver_path)
            )

def material_pdm_silver_to_gold(spark):
    
    df = spark.read.format("delta").load(material_silver_path)
    df.createOrReplaceTempView("material_silver")

    (spark.sql("""
              select codigoPdm, 
              nomePdm, 
              codigoClasse, 
              descricaoClasse, 
              codigoGrupo, 
              descricaoGrupo, 
              sum(case when itemSuspenso = true then 1 else 0 end) qtd_item_suspenso,
              sum(case when itemSustentavel = true then 1 else 0 end) qtd_item_sustentavel,
              sum(case when statusItem = true then 1 else 0 end) qtd_item_ativo,
              sum(case when statusItem = false then 1 else 0 end) qtd_item_inativo,
              count(*) qtd_total
              
              from material_silver
              
              group by codigoPdm, nomePdm, codigoClasse, descricaoClasse, codigoGrupo, descricaoGrupo
              
              """).write
            .format("delta")
            .mode("overwrite")
            .save(material_pdm_gold_path)
    )
    
    

# classes_landing_to_bronze()
# grupos_landing_to_bronze()
# material_landing_to_bronze()
