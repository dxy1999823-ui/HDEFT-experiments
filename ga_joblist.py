'''(1)use oringinal rank to get jobList
   (2)use ga to find the best order to allocate GPU_resource use HEFT
   (assume that one job occupies one gpu_resource)
'''
from typing import List
from entity import job, DAG, Individual
import random
import copy
import bisect
import itertools


'''def getlist(job_list: List[job]):
    sorted(job_list, k=lambda x: x.rank)
    job_list[0].rank = 0
    for i in range(1, job_list):
        if job_list[i].rank == job_list[i - 1].rank:
            job_list[i].rank = job_list[i - 1].rank
        else:
            job_list[i].rank = i'''


def init_individual(job_list: List[job]):
    def print_jobs(explan:str, jobs:List[job]):
        print(explan, end=" ")
        for JOB in jobs:
            print(JOB.job_id, end=" ")
        print()
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    #print_jobs("original_order:", job_list)
    group_list = [[]for _ in range(len(job_list))]#important 2D list definition
    for i in range(len(job_list)):
        #print("job_id:",job_list[i].job_id)
        #print("job_list[i].rank:",job_list[i].rank)
        group_list[job_list[i].rank].append(job_list[i])
        #print("time:",i)
    for j in range(len(group_list)):
        print("Group:", j)
        for k in range(len(group_list[j])):
            print(group_list[j][k].job_id, end=" ")
        print()
    print()
    for j in range(len(group_list)):
        if len(group_list[j]) > 1:
            print_jobs("before:",group_list[j])
            random.shuffle(group_list[j])
            #print("after:",group_list[j])
    result = []
    for k in range(len(group_list)):
        for m in range(len(group_list[k])):
            result.append(group_list[k][m])
    '''for j in range(len(group_list)):
        print("Group:", j)
        for k in range(len(group_list[j])):
            print(group_list[j][k].job_id, end=" ")
        print()
    print()
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')'''
    print("after_order:", end=" ")
    for JOB in result:
        print(JOB.job_id, end=" ")
    print()
    return Individual(result), group_list


def cross_over(group: List[Individual], job_num: int):
    cross_orders = random.sample(list(range(len(group))), len(group))

    cross_couples = [(cross_orders[i], cross_orders[i + 1]) for i in range(0, len(cross_orders), 2)]
    #print(type(job_num))
    for cross_couple in cross_couples:
        fos = group[cross_couple[0]].job_list
        sos = group[cross_couple[1]].job_list
        num_list = [i for i in range(0, job_num)]
        cross_point = random.choice(num_list)
        fos, sos = fos[:cross_point] + sos[cross_point:], sos[:cross_point] + fos[cross_point:]

        group[cross_couple[0]].job_list = fos
        group[cross_couple[1]].job_list = sos


def mutation_process(group: List[Individual], order_group: List[List[job]]):
    def mutation(x: int, cl: List[int]) -> int:
        cl.remove(x)
        return random.choice(cl)

    for k in range(len(group)):
        individual = group[k]
        individual_ordergroup = order_group[k]
       # ios = individual.job_list
        can_mutation = []
        #print("type of order_group:",type(order_group[0][0][0]))
        for i in range(0, len(individual_ordergroup)):
            '''print("Individual:", i)
            for j in range(len(order_group[i])):
                print("Group:", j)
                for k in order_group[i][j]:
                    print(k.job_id, end=" ")
                print()
            print()
            print("&&&&&&&&&&&&&&&&&&&")'''
            if len(individual_ordergroup[i]) > 1:
                #print("%%%%%%%%%%%%%%%%%%%%%")
                can_mutation.append(i)
       # print("length of individual_ordergroup:",len(individual_ordergroup))
        '''print("canmutation:",can_mutation)
        mutation_group_index = random.choice(can_mutation)
        mutation_point = random.choice(individual_ordergroup[mutation_group_index])
        index = mutation_group_index + 1
        while len(individual_ordergroup[index]) == 0:
            index += 1
        mutation_point.rank = mutation(mutation_point.rank, range(mutation_group_index,index))'''
        for index in can_mutation:
            random.shuffle(individual_ordergroup[index])
        final_job_list = []
        for Index in range(len(individual_ordergroup)):
            for JOB in individual_ordergroup[Index]:
                final_job_list.append(JOB)
        individual.job_list = final_job_list
        #individual.job_list = sorted(individual.job_list, k=lambda x: x.rank)


def selection(group: List[Individual]) -> List[Individual]:
    def adaptability_func(total_time: float) -> float:
        return 1 / total_time

    adaptability_list = []
    for individual in group:
        adaptability_list.append(adaptability_func(individual.total_time))
    all_adaptability = sum(adaptability_list)

    isp = [a / all_adaptability for a in adaptability_list]
    cp = list(itertools.accumulate(isp))
    rn = [random.random() for _ in range(len(group))]

    return [copy.deepcopy(group[bisect.bisect_left(cp, rn[i])]) for i in range(len(group))]


def preferential_admission(origin_group: List[Individual], change_group: List[Individual]) -> List[Individual]:
    return sorted(origin_group + change_group, key=lambda i: i.total_time)[:len(origin_group)]


def get_original_order(dag: DAG):
    caled = [False] * len(dag.job_list)
    #print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    #print("caled:",caled)
    cal_num = 0
    for enter_job in dag.enters:
        #print("*****************************************************")
        #print("enter_job:",enter_job.job_id)
        enter_job.rank = 0
        caled[enter_job.job_id] = True
        cal_num += 1
    while cal_num < len(dag.job_list):
        for i in range(len(dag.job_list)):
            if not caled[i]:
                cancal = True
                for pred_job in dag.job_list[i].pred_jobs:
                    if not caled[pred_job.job_id]:
                        cancal = False
                        break
                if cancal:
                    dag.job_list[i].rank = max(
                        dag.job_list[i].pred_jobs[j].rank for j in range(len(dag.job_list[i].pred_jobs))) + 1
                    cal_num += 1
                    caled[i] = True
    print("-----------------------------------------")
    for JOB in dag.job_list:
        print(JOB.rank)
    print("-----------------------------------------")
