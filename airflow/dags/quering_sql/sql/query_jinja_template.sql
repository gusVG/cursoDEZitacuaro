-- usando Jinja template para introducir los parámetros desde el operador
SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC '{{ params.begin_date }}' AND '{{ params.end_date }}'