import postgresql
from random import randint
import time

#Função para inserir 1 milhão de tuplas com valores inteiros aleatórios e registrar a duração da operação
def insercao():

	db = postgresql.open("pq://postgres:postgres@localhost/test")
	insert = db.prepare("insert into val values ($1, $2)") 
	
	insercao_inicio = time.time()
	for int in range(1000000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		insert (val1, val2)
	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração de 1.000.000 inserções: %f" %insercao_duracao)

	db.close()

#Função para inserir 1 mil, 10 mil, 100 mil e 1 milhão de tuplas de valores inteiros aleatórios e registrar a duração de cada operação
def insercao_gradual():

	db = postgresql.open("pq://postgres:postgres@localhost/test")
	insert = db.prepare("insert into val values ($1, $2)") 

	insercao_inicio = time.time()
	for int in range(1000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		insert (val1, val2)
	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração de 1.000 inserções: %f" %insercao_duracao)

	insercao_inicio = time.time()
	for int in range(10000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		insert (val1, val2)
	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração de 10.000 inserções: %f" %insercao_duracao)

	insercao_inicio = time.time()
	for int in range(100000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		insert (val1, val2)
	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração de 100.000 inserções: %f" %insercao_duracao)

	insercao_inicio = time.time()
	for int in range(1000000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		insert (val1, val2)
	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração de 1.000.000 inserções: %f" %insercao_duracao)

	db.close()

#Função para consultar tuplas com val1 entre 0 e 10, entre 0 e 50 e entre 0 e 100, registrando o tempo de duração de cada operação
def consulta():
	db = postgresql.open("pq://postgres:postgres@localhost/test")
	consulta = db.prepare("select val1 from val where val1 >= 0 and val1 <= 10") 
	consulta_inicio = time.time()
	consulta()
	consulta_fim = time.time()
	consulta_duracao = consulta_fim - consulta_inicio
	print("Duração da consulta por val1 entre 0 e 10: %f" %consulta_duracao)

	consulta = db.prepare("select val1 from val where val1 >= 0 and val1 <= 50") 
	consulta_inicio = time.time()
	consulta()
	consulta_fim = time.time()
	consulta_duracao = consulta_fim - consulta_inicio
	print("Duração da consulta por val1 entre 0 e 50: %f" %consulta_duracao)

	consulta = db.prepare("select val1 from val where val1 >= 0 and val1 <= 100") 
	consulta_inicio = time.time()
	consulta()
	consulta_fim = time.time()
	consulta_duracao = consulta_fim - consulta_inicio
	print("Duração da consulta por val1 entre 0 e 100: %f" %consulta_duracao)

	db.close()

#Função para realizar a alteração de todos as tuplas, atribuindo novos valores aleatórios para val1 e val2, registrando o tempo de duração da operação
def alteracao():
	db = postgresql.open("pq://postgres:postgres@localhost/test")
	update = db.prepare("update val set val1 = $1, val2 = $2") 

	val1 = randint(0,100)
	val2 = randint(0,100)
	update_inicio = time.time()
	update (val1, val2)
	update_fim = time.time()
	update_duracao = update_fim - update_inicio
	print(update_duracao)

	db.close()

#Função para realizar a exclusão de todas as tuplas que contam com val1 entre 0 e 10, entre 0 e 50 e entre 0 e 100, registrando a duração de cada operação nas tabelas
def exclusao():
	db = postgresql.open("pq://postgres:postgres@localhost/test")
	delete = db.prepare("delete from val_copia where val1 >= 0 and val1 <= 10") 
	delete_inicio = time.time()
	delete()
	delete_fim = time.time()
	delete_duracao = delete_fim - delete_inicio
	print("Duração da exclusão de linhas com val1 entre 0 e 10: %f" %delete_duracao)

	delete = db.prepare("delete from val_copia2 where val1 >= 0 and val1 <= 50") 
	delete_inicio = time.time()
	delete()
	delete_fim = time.time()
	delete_duracao = delete_fim - delete_inicio
	print("Duração da exclusão de linhas com val1 entre 0 e 50: %f" %delete_duracao)

	delete = db.prepare("delete from val_copia3 where val1 >= 0 and val1 <= 100") 
	delete_inicio = time.time()
	delete()
	delete_fim = time.time()
	delete_duracao = delete_fim - delete_inicio
	print("Duração da exclusão de linhas com val1 entre 0 e 100: %f" %delete_duracao)

	db.close()