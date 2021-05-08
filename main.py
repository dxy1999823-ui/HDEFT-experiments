from HDEFT_allocation import allocation
from datamake import datamake, DAGMake
from entity import job, gpu, DAG
from experiment import ga_execution
from typing import List
import copy
import numpy as np
from original_allocation import original_allocation

def experiment(job_num, gpu_num):
    file = './DAG.txt'
    job_list = []
    gpu_list = []
    for i in range(job_num):
        JOB = job(i)

        job_list.append(JOB)

    for j in range(gpu_num):
        GPU = gpu(j,[])
        gpu_list.append(GPU)
    training_data = datamake(job_num, gpu_num)
    #training_data = np.loadtxt('trainingdata.txt')
    DAG_detail=DAGMake(job_num)
    '''training_data = [
        [10,11,12,13,14],
        [12,13,14,15,16],
        [14,16,18,20,22],
        [16,18,20,22,24],
        [4,6,8,10,12],
        [6,8,10,12,14],
        [24,26,28,30,32],
        [34,36,38,40,42]
    ]'''
    #DAG_detail = np.loadtxt('DAG.txt')
    dag = DAG(DAG_detail, job_list)
    for JOB in job_list:
        JOB.get_pred_jobs(dag)
        JOB.get_succ_jobs(dag)
        JOB.get_aver_cost(training_data)
    dag.get_enter()
    '''for Job in dag.enters:
        print("***************************")
        print(Job.job_id)'''
    dag.get_exit()
    dag.cal_aest()
    dag.cal_alst()
    dag.cal_left_time()
    dag.get_critical_path()
    job_queue = dag.pro_job_queue()#baseline order
    #print("Is legal???????",dag.is_legal(job_queue))
    #print("job_queue:",end = " ")
    #for j in job_queue:
        #print(j.job_id,end = " ")
    #print()
    job_queue1 = copy.deepcopy(job_queue)
    gpu_list1 = copy.deepcopy(gpu_list)
    job_queue2 = copy.deepcopy(job_queue)
    gpu_list2 = copy.deepcopy(gpu_list)
    job_queue3 = copy.deepcopy(job_queue)
    gpu_list3 = copy.deepcopy(gpu_list)
    original_time = original_allocation(job_queue1, gpu_list1, training_data, dag)
    base_time = allocation(job_queue2, gpu_list2, training_data, dag)
    ga_result = ga_execution(job_queue3, gpu_list3, training_data, dag, 16, 15)
    print('original_result:', original_time)
    print('baseline_result:', base_time)
    print('ga_result:', ga_result)
    print("baseline_plan:")
    for resource in gpu_list2:
        print("resource_id:"+str(resource.gpu_id)+":")
        for JOB in resource.job_list:
            print(JOB.job_id, end = " ")
        print()
    for JOb in job_queue2:
        print("job_id:"+str(JOb.job_id),end = "    ")
        print("ass_gpu_id:"+str(JOb.ass_gpu.gpu_id))
        print("ga_experiment_plan:")
    for resource3 in gpu_list3:
        print("resource_id:"+str(resource3.gpu_id)+":")
        for JOB3 in resource3.job_list:
            print(JOB3.job_id,end = " ")
        print()


if __name__ == '__main__':
    experiment(50, 10)
