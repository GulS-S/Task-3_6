from datetime import datetime
import random

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator



def hello():
    print("Airflow")
    
def two_random_number():
    print(random.randint(0, 10))
    print(random.randint(0, 10))    
    
def number_file():
    for i in range(2):
        n_1 = random.randint(0, 10)
        n_2 = random.randint(0, 10)
        with open('input.txt', 'a', encoding='utf-8') as file:
            file.write(f'{n_1} {n_2}\n')    
    
    
def s_file():  
   s_1 = 0
   s_2 = 0
   with open('input.txt', 'r', encoding='utf-8') as file:
        for i in file:
            i = i.split()
            s_1 += int(i[0])
            s_2 += int(i[1])
   difference = s_1 - s_2
   with open('input.txt', 'a', encoding='utf-8') as file:
       file.write(str(difference))
  
   
    
    
    
# A DAG represents a workflow, a collection of tasks
with DAG(dag_id ="first_dag", start_date=datetime(2022, 11, 1), schedule="1-5/1 * * * *") as dag:

    # Tasks are represented as operators
    bash_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task = PythonOperator(task_id="world", python_callable=hello)
    
    
    python_task_a = PythonOperator(task_id="two_random_number", python_callable=two_random_number)
    python_task_c = PythonOperator(task_id="number_file", python_callable=number_file)
    python_task_d = PythonOperator(task_id="s_file", python_callable=s_file)
    
    # Set dependencies between tasks
    bash_task >> python_task >> python_task_a >> python_task_c >> python_task_d