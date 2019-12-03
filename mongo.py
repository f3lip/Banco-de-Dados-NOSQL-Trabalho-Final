from pymongo import MongoClient
from random import randint
import time

#Realizar a conexão com o MongoDB
cliente = MongoClient('localhost', 27017)
cliente = MongoClient('mongodb://localhost:27017/')
banco = cliente.insert
valores = banco.valores

#Função para inserir 1 milhão de tuplas de valores inteiros aleatórios e registrar a duração da operação
def insercao():

	insercao_inicio = time.time()

	for int in range(1000000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		documento = {"val1": val1, "val2": val2}
		valores.insert_one(documento)

	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração da inserção de 1 milhão de documentos: %f" %insercao_duracao)

#Função para inserir 1 mil, 10 mil, 100 mil e 1 milhão de tuplas de valores inteiros aleatórios e registrar a duração de cada operação
def insercao_gradual():

	insercao_inicio = time.time()

	for int in range(1000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		documento = {"val1": val1, "val2": val2}
		valores.insert_one(documento)

	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração da inserção de 1.000 documentos: %f" %insercao_duracao)

	insercao_inicio = time.time()

	for int in range(10000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		documento = {"val1": val1, "val2": val2}
		valores.insert_one(documento)

	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração da inserção de 10.000 documentos: %f" %insercao_duracao)

	insercao_inicio = time.time()

	for int in range(100000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		documento = {"val1": val1, "val2": val2}
		valores.insert_one(documento)

	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração da inserção de 100.000 documentos: %f" %insercao_duracao)

	for int in range(1000000):
		val1 = randint(0,100)
		val2 = randint(0,100)
		documento = {"val1": val1, "val2": val2}
		valores.insert_one(documento)

	insercao_fim = time.time()
	insercao_duracao = insercao_fim - insercao_inicio
	print("Duração da inserção de 1.000.000 documentos: %f" %insercao_duracao)

#Função para consultar documentos com val1 entre 0 e 10, entre 0 e 50 e entre 0 e 100, registrando a duração de cada operação individual
def consulta():
	consulta_inicio = time.perf_counter()
	consulta = valores.find({"val1": {'$gte': 0, '$lte': 10}})
	consulta_fim = time.perf_counter()
	consulta_duracao = consulta_fim - consulta_inicio
	print("Duração da consulta aos documentos com val1 entre 0 e 10: %f" %consulta_duracao)

	consulta_inicio = time.perf_counter()
	consulta = valores.find({"val1": {'$gte': 0, '$lte': 50}})
	consulta_fim = time.perf_counter()
	consulta_duracao = consulta_fim - consulta_inicio
	print("Duração da consulta aos documentos com val1 entre 0 e 50: %f" %consulta_duracao)

	consulta_inicio = time.perf_counter()
	consulta = valores.find({"val1": {'$gte': 0, '$lte': 100}})
	consulta_fim = time.perf_counter()
	consulta_duracao = consulta_fim - consulta_inicio
	print("Duração da consulta aos documentos com val1 entre 0 e 100: %f" %consulta_duracao)

#Função para realizar a alteração em todos os documentos, inserindo um novo valor aleatório para val1 e val2, registrando a duração da operação
def alteracao():
	val1 = randint(0,100)
	val2 = randint(0,100)
	update_inicio = time.perf_counter()
	update = valores.update_many({}, {"$set": {"val1": val1, "val2": val2}})
	update_fim = time.perf_counter()
	update_duracao = update_fim - update_inicio
	print("Tempo para alterar todos os documentos: %f" %update_duracao)

#Função para excluir todos os documentos que contam com val1 entre 0 e 10, registrando a duração da operação
def exclusao1():
	delete_inicio = time.perf_counter()
	valores.delete_many({"val1": {'$gte': 0, '$lte': 10}})
	delete_fim = time.perf_counter()
	delete_duracao = delete_fim  - delete_inicio
	print("Tempo para deletar documentos com val1 entre 0 e 10: %f" %delete_duracao)

#Função para excluir todos os documentos que contam com val1 entre 0 e 50, registrando a duração da operação
def exclusao2():
	delete_inicio = time.perf_counter()
	valores.delete_many({"val1": {'$gte': 0, '$lte': 50}})
	delete_fim = time.perf_counter()
	delete_duracao = delete_fim  - delete_inicio
	print("Tempo para deletar documentos com val1 entre 0 e 50: %f" %delete_duracao)

#Função para excluir todos os documentos que contam com val1 entre 0 e 100, registrando a duração da operação
def exclusao3():
	delete_inicio = time.perf_counter()
	valores.delete_many({"val1": {'$gte': 0, '$lte': 100}})
	delete_fim = time.perf_counter()
	delete_duracao = delete_fim  - delete_inicio
	print("Tempo para deletar documentos com val1 entre 0 e 100: %f" %delete_duracao)