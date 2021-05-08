from __future__ import annotations

from dataclasses import dataclass
from typing import List
import numpy as np

@dataclass
class job:
    job_id: int
    aver_cost: int = 0
    pred_jobs: List[job] = None
    succ_jobs: List[job] = None
    aest: float = 0
    alst: float = 0
    left_time: float = 0
    ass_gpu: gpu = None
    rank: int = 0

    def get_succ_jobs(self, DAG_graph: DAG):
        self.succ_jobs = []
        for i in range(len(DAG_graph.DAG_detail[0])):
            if DAG_graph.DAG_detail[self.job_id][i] != 0:
                self.succ_jobs.append(DAG_graph.job_list[i])

    def get_pred_jobs(self, DAG_graph: DAG):
        self.pred_jobs = []
        for i in range(len(DAG_graph.DAG_detail[0])):
            if DAG_graph.DAG_detail[i][self.job_id] != 0:
                self.pred_jobs.append(DAG_graph.job_list[i])

    def get_aver_cost(self,training_data:List[List[float]]):
        total_time = 0
        for time in training_data[self.job_id]:
            total_time += time
        self.aver_cost = total_time/len(training_data[0])



@dataclass
class DAG:
    DAG_detail: np.ndarray
    job_list: List[job]
    enters: List[job] = None
    exits: List[job] = None
    critical_list: List[job] = None
    ass_gpu: gpu = None

    def get_enter(self):
        self.enters = []
        for job in self.job_list:
            job.get_pred_jobs(self)
            if len(job.pred_jobs) == 0:
                self.enters.append(job)

    def get_exit(self):
        self.exits = []
        for job in self.job_list:
            job.get_succ_jobs(self)
            if len(job.succ_jobs) == 0:
                self.exits.append(job)

    def cal_aest(self):
        caled = [False] * len(self.job_list)
        #print("list_length:",len(caled))
        cal_num = 0
        for enter_job in self.enters:
            enter_job.aest = 0
            #print("enter_job.job_id:", enter_job.job_id)
            caled[enter_job.job_id] = True
            cal_num += 1
        while cal_num < len(self.job_list):
            for i in range(len(self.job_list)):
                if not caled[i]:
                    cancal = True
                    pred_value = []
                    for pred_job in self.job_list[i].pred_jobs:
                        if not caled[pred_job.job_id]:
                            cancal = False
                            break
                        else:
                            pred_value.append(pred_job.aver_cost + self.DAG_detail[pred_job.job_id][
                                self.job_list[i].job_id] + pred_job.aest)

                    if cancal:
                        #print("pred_value:", pred_value)
                        self.job_list[i].aest = max(pred_value)
                        caled[i] = True
                        cal_num += 1

    def cal_alst(self):
        caled = [False] * len(self.job_list)
        cal_num = 0
        for exit_job in self.exits:
            #print("exit_job.job_id:",exit_job.job_id)
            exit_job.alst = exit_job.aest
            caled[exit_job.job_id] = True
            cal_num += 1
        while cal_num < len(self.job_list):
            for i in range(len(self.job_list)):
                if not caled[i]:
                    cancal = True
                    succ_value = []
                    for succ_job in self.job_list[i].succ_jobs:
                        if not caled[succ_job.job_id]:
                            cancal = False
                            break
                        else:
                            succ_value.append(- self.job_list[i].aver_cost - self.DAG_detail[self.job_list[i].job_id][
                                succ_job.job_id] + succ_job.alst)
                    if cancal:
                        self.job_list[i].alst = min(succ_value)
                        cal_num += 1
                        caled[i] = True

    def cal_left_time(self):
        for Job in self.job_list:
            Job.left_time = Job.alst - Job.aest

    def get_critical_path(self):
        self.critical_list = []
        for Job in self.job_list:
            if Job.left_time == 0:
                self.critical_list.append(Job)

    def pro_job_queue(self):
        '''def print_job(name:str,job_list: List[job]):
            print(name+":")
            for JOB in job_list:
                print(JOB.job_id, end=" ")
            print()'''
        #for JOb in self.job_list:
         #   print(str(JOb.job_id)+": aest:"+str(JOb.aest)+" alst:"+str(JOb.alst))
        sort_job_list = self.job_list
        sort_job_list = sorted(sort_job_list,key=lambda x: x.alst, reverse=True)
        job_stack = []
        result = []
        for Job in sort_job_list:
            if Job in self.critical_list:
                job_stack.append(Job)

        #print("critical_path:")
        #for j in self.critical_list:
            #print(j.job_id, end=" ")
        #print()
        #print_job("job_stack", job_stack)
        #print()
        time = 0
        while len(job_stack) > 0:

        #time = 0
        #while time < 5:
            time += 1
            #print("time to enter the cycle:", time)
            #print("before:",len(job_stack))
            stack_top = job_stack.pop()
            #print("after:",len(job_stack))
            if len(stack_top.pred_jobs) == 0:
                #print("condition1")
                result.append(stack_top)
            else:
                canpush = True
                for pred_job in stack_top.pred_jobs:
                    if pred_job not in result:
                        #print("meet the demand")
                        canpush = False
                        break
                if canpush:
                    #print("condition2")
                    result.append(stack_top)
                else:
                    #print("condition3")
                    job_stack.append(stack_top)
                    #print("after1:", len(job_stack))
                    pre_jobs = sorted(stack_top.pred_jobs, key=lambda x: x.left_time)
                    for JOB in pre_jobs:
                        if JOB not in job_stack and JOB not in result:
                            #print("meet the demands")
                            job_stack.append(JOB)
                            #print_job("append_result", job_stack)
                            #print()
                            break
            #print_job("new_job_stack", job_stack)
            #print_job("result",result)
            #print("------------------------------------")
            #for index in range(len(result)):
                #result[index].rank = index
        for index in range(len(result)):
            result[index].rank = index
        return result

    def is_legal(self,job_list:List[job]):
        result = True
        rank = [0]*len(job_list)
        for index in range(len(job_list)):
            rank[job_list[index].job_id] = index
        for i in range(len(job_list)):
            for j in range(len(job_list)):
                if self.DAG_detail[i][j] != 0 and (rank[i] > rank[j] or rank[i] == rank[j]):
                    result = False
        return result


@dataclass
class gpu:
    gpu_id: int
    job_list: List[job]
    ava: float = 0  # time can use

    def cal_gpu_time(self, training_data: List[List[float]], dag: DAG):
        total_time = 0
        for index in range(len(self.job_list)):
            cur_job = self.job_list[index]
            total_time += training_data[cur_job.job_id][self.gpu_id]
            if index != len(self.job_list)-1:
                succ_job = self.job_list[index+1]
                if succ_job not in cur_job.succ_jobs:
                    total_time += dag.DAG_detail[cur_job.job_id][succ_job.job_id]
        return total_time


@dataclass
class Individual:
    job_list: List[job] = None
    total_time: float = 0


@dataclass
class TrainingData:
    epoch_num: int
    epoch_time: float