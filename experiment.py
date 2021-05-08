from ga_joblist import *
from typing import List, Dict
from entity import *
from HDEFT_allocation import allocation
import numpy as np


def ga_execution(job_list: List[job],
                 gpu_list: List[gpu],
                 data: np.ndarray,
                 dag: DAG,
                 individual_num: int,
                 iteration_times: int):
    #problem: the input rank of job_list
    get_original_order(dag)
    #print("??????????????????????????????")
    #for JOB in dag.job_list:
    #    print(JOB.rank)
    job_nums = len(job_list)
    '''for index in range(len(job_list)):
        print(job_list[index].job_id,end=" ")
        print(dag.job_list[index].job_id)'''
    for index in range(len(job_list)):
        job_list[index].rank = dag.job_list[job_list[index].job_id].rank
    #job_orders = list(range(1, job_nums + 1))
    new_group = []
    order_group = []
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    #print(dag.is_legal(job_list))
    #for JOB in job_list:
        #print(JOB.rank)
    for _ in range(individual_num):
        i ,o_g = init_individual(job_list)
        new_group.append(i)
        #print("is Individual legal?:",dag.is_legal(i.job_list))
        '''if len(o_g[6])!=0:
            print("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")'''
        order_group.append(o_g)
    #new_group = [init_individual(job_list) for _ in range(individual_num)]
    #print(type(new_group[0]))
    #print(new_group[0].total_time)
    #print("****************")
    #print(new_group[0])

    for individual in new_group:
        for GPU in gpu_list:
            GPU.job_list = []
            GPU.ava = 0
        for JOB in job_list:
            JOB.ass_gpu = None
        #print(individual.total_time)
        individual.total_time = allocation(individual.job_list, gpu_list, data, dag)

    for time in range(iteration_times):
        after_group = selection(new_group)
        print(time)
        cross_over(after_group, job_nums)
        mutation_process(after_group, order_group)
        for individual in after_group:
            for JOB in individual.job_list:
                print(JOB.job_id,end = " ")
            print()
            for GPU in gpu_list:
                GPU.job_list = []
                GPU.ava = 0
            for JOB in job_list:
                JOB.ass_gpu = None
            individual.total_time = allocation(individual.job_list, gpu_list, data, dag)
            #individual.total_time = 0
        new_group = preferential_admission(new_group, after_group)
    '''if used_slice:
        print("ga_execution with slice:")
    else:
        print("ga_execution:")'''
    #best_group = new_group[0]
    return new_group[0].total_time
        #/, new_group[0].plan.utilization_rate