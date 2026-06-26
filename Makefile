.PHONY: help setup pyspark-test pyspark-run sql-build sql-lint clean

help:
	@echo "setup         Instala dependencias de ambos proyectos"
	@echo "pyspark-test  Corre tests del proyecto PySpark"
	@echo "pyspark-run   Ejecuta el pipeline PySpark completo (local)"
	@echo "sql-build     dbt seed + run + test"
	@echo "sql-lint      sqlfluff lint de los modelos"

setup:
	cd project-pyspark && pip install -e ".[dev]"
	cd project-sql && pip install -r requirements.txt && dbt deps

pyspark-test:
	cd project-pyspark && PYTHONPATH=src pytest

pyspark-run:
	cd project-pyspark && PYTHONPATH=src python -m olist_seg.cli --build-all

sql-build:
	cd project-sql && dbt seed && dbt run && dbt test

sql-lint:
	cd project-sql && sqlfluff lint models --dialect snowflake --templater dbt

clean:
	rm -rf project-pyspark/data/bronze project-pyspark/data/silver project-pyspark/data/gold \
	       project-pyspark/artifacts project-pyspark/mlruns project-sql/target project-sql/dbt_packages
