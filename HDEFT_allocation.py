from typing import List
from entity import job, gpu, DAG


def allocation(job_queue: List[job], gpu_list: List[gpu], training_data: List[List[float]], dag: DAG):
    def find_job(id: int, joblist: List[job]):
        for j in joblist:
            if j.job_id == id:
                return j
    #print("**************************************************888")
    job_num = len(job_queue)
    gpu_num = len(gpu_list)
    allo_detail = [[0] * job_num for _ in range(gpu_num)] # more unreliable than gpu.job_list
    tst = [[0.000] * job_num for _ in range(gpu_num)]
    tft = [[0.000] * job_num for _ in range(gpu_num)]
    slot = [[0.000] * job_num for _ in range(gpu_num)]
    cur_index = 0
    result = dag.is_legal(job_queue)
    #print("Is job order legal?:",result)
    while cur_index < job_num :
        print("*************************************")
        print("JOB:",job_queue[cur_index].job_id)
        if job_queue[cur_index].job_id == 5:
            print("?????????????????????????????????????????????????????????")
        print("pred_job:",end =" ")
        for JOB in job_queue[cur_index].pred_jobs:
            print(JOB.job_id,end = " ")
        print()
        cur_job = job_queue[cur_index]
        earlist = 99999999
        #problem1
        insert_pred_job = [[]for _ in range(gpu_num)] # store pred_jobs which can be inserted in slot when assigned in different gpu
        #problem: the definition of insert_pred_job check the content of insert_pred_job
        for gpu_resource in gpu_list:
            print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
            print("gpu_resource:",gpu_resource.gpu_id)
            #print("job_list:",end = " ")
            #for JOB in gpu_resource.job_list:
            #    print(JOB.job_id,end = " ")
            #print()
            if len(cur_job.pred_jobs) == 0:
                cal_tst(cur_job,gpu_resource,tst,tft,dag,training_data)
                tft[gpu_resource.gpu_id][cur_job.job_id] = tst[gpu_resource.gpu_id][cur_job.job_id] + \
                                                           training_data[cur_job.job_id][gpu_resource.gpu_id]
                '''if cur_job.job_id ==1 and gpu_resource.gpu_id ==3:
                    print("tst[3][1]:",tst[3][1])
                    print("tft[3][1]:",tft[3][1])'''
            else:
                pred_list = {}
                for pred_job in cur_job.pred_jobs:
                    value = tft[pred_job.ass_gpu.gpu_id][pred_job.job_id] + dag.DAG_detail[pred_job.job_id][
                        cur_job.job_id]
                    pred_list.setdefault(pred_job.job_id, value)
                sorted(pred_list.items(), key=lambda x: x[1], reverse=True)
                # cal_tst
                cal_tst(cur_job, gpu_resource, tst, tft, dag, training_data)
                # cal_tft
                cal_tft(cur_job, gpu_resource, tst, tft, training_data)
                # cal_slot
                slot[gpu_resource.gpu_id][cur_job.job_id] = tst[gpu_resource.gpu_id][cur_job.job_id] - gpu_resource.ava
                index = 0
                original_ass_gpus = []
                while index < len(cur_job.pred_jobs):
                   # print("enter--------------------------")

                    pred_list_id = list(pred_list.keys())
                    critical_pred = find_job(pred_list_id[index], job_queue)
                    original_ass_gpus.append( critical_pred.ass_gpu)

                    #print("tft[0][1]",tft[0][1])
                    if can_dedicate(cur_job, critical_pred, gpu_resource, slot, tst, tft, training_data, dag):
                        print("can_dedicate:",critical_pred.job_id)

                        insert_slot(gpu_resource,critical_pred, cur_job, slot, tst,tft,dag, training_data)  # problem:dedicate or migration
                        allo_detail[gpu_resource.gpu_id][pred_job.job_id] = 1
                        cal_tft(cur_job, gpu_resource, tst, tft, training_data)
                        #print("add insert job")
                        insert_pred_job[gpu_resource.gpu_id].append(critical_pred)
                        '''print("insert_pred_job:",gpu_resource.gpu_id)
                        for JOB in insert_pred_job[gpu_resource.gpu_id]:
                            print(JOB.job_id,end = " ")
                        print()'''
                        gpu_resource.job_list.pop()
                        #critical_pred.ass_gpu = original_ass_gpu
                    index += 1
                    '''else:
                        break  # problem:break or consider the less critical job'''
                #print("tst[0][5]",tst[0][5])
                cal_tft(cur_job, gpu_resource, tst, tft, training_data)
            #critical_pred.ass_gpu = original_gpu problem: how to return without influencing other tft
            print("[[[return original ass_gpu]]]")
            for i in range(len(cur_job.pred_jobs)):
                cur_job.pred_jobs[i].ass_gpu = original_ass_gpus[i]
                print("JOB" + str(cur_job.pred_jobs[i].job_id) + ".ass_gpu:" + str(original_ass_gpus[i].gpu_id))
            print("[[[return original ass_gpu]]]")
        # find min
        print("cur_job:",cur_job.job_id)
        '''print("[[[return original ass_gpu]]]")
        for i in range(len(cur_job.pred_jobs)):
            cur_job.pred_jobs[i].ass_gpu = original_ass_gpus[i]
            print("JOB"+str(cur_job.pred_jobs[i].job_id)+".ass_gpu:"+str(original_ass_gpus[i].gpu_id))
        print("[[[return original ass_gpu]]]")'''
        #print(job_queue[0].ass_gpu.gpu_id)
        for i in range(gpu_num):
            print("gpu_id:" + str(i) + " " + str(tft[i][cur_job.job_id]))
            if tft[i][cur_job.job_id] < earlist:
                earlist = tft[i][cur_job.job_id]
                min_index = i
            print("gpu.job_list:", end=" ")
            for JOB in gpu_list[i].job_list:
                print(JOB.job_id,end = " ")
            print()
            print("________________________________")

        allo_detail[min_index][cur_job.job_id] = 1  # allocate
        cur_job.ass_gpu = gpu_list[min_index]  # allocate
        dag.job_list[cur_job.job_id].ass_gpu = gpu_list[min_index]
        '''print("insert_job____________________________________________")
        for insertjob in insert_pred_job[min_index]:
            print(insertjob.job_id,end = " ")
        print()'''
        if len(insert_pred_job[min_index]) != 0:
            for insert_job in insert_pred_job[min_index]:
                gpu_list[min_index].job_list.append(insert_job)
        #print("min_index-----------------------------",min_index)
        gpu_list[min_index].job_list.append(cur_job)
        print("cur_job:",cur_job.job_id)
        print("ass_gpu:",cur_job.ass_gpu.gpu_id)

        #gpu_list[min_index].job_list.append(cur_job)
        gpu_list[min_index].ava = tft[min_index][cur_job.job_id]
        cur_index += 1
        #print("ava:",gpu_list[1].ava)
        update_ava(gpu_list,tft)
        #print("********************************************")
    dag.job_list = sorted(job_queue, key=lambda x: x.job_id)
    #print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

    return cal_total_time(dag, tft)


def update_ava(gpu_list:List[gpu],tft:List[List[float]]):
    for GPU in gpu_list:
        if len(GPU.job_list) > 0 :
            last_job = GPU.job_list[-1]
            GPU.ava = tft[GPU.gpu_id][last_job.job_id]
        else:
            GPU.ava = 0


def cal_tst(cur_job: job, cur_gpu: gpu, tst: List[List[float]], tft: List[List[float]], dag: DAG,
            training_data: List[List[float]]):
    new_tst = 0.000
    for pred_job in cur_job.pred_jobs:
        if pred_job in cur_gpu.job_list:  # in the same gpu communication cost = 0
            value = tft[cur_gpu.gpu_id][pred_job.job_id]
            print("enter1:",value)
            '''value = tst[pred_job.ass_gpu.gpu_id][pred_job.job_id] + training_data[pred_job.ass_gpu.gpu_id][
                cur_job.job_id]#problem:yi si lun wen gong shi you wu'''
        else:
            value = tft[pred_job.ass_gpu.gpu_id][pred_job.job_id] + \
                    dag.DAG_detail[pred_job.job_id][cur_job.job_id]
            print("pred_job.ass_gpu.gpu_id",pred_job.ass_gpu.gpu_id)
            print("enter2:",value)
            '''if cur_job.job_id == 3 and cur_gpu.gpu_id == 0:
                print("pred_job.ass_gpu:",pred_job.ass_gpu.gpu_id)
                print("tft:",tft[pred_job.ass_gpu.gpu_id][pred_job.job_id])
                print("dag:",dag.DAG_detail[pred_job.job_id][cur_job.job_id])'''
            #print("pred_job:",pred_job.job_id)
            #print("pred_job.ass_gpu:",pred_job.ass_gpu.gpu_id)

            '''value = tst[pred_job.ass_gpu.gpu_id][pred_job.job_id] + training_data[pred_job.ass_gpu.gpu_id][cur_job.job_id] + \
                dag.DAG_detail[pred_job.job_id][cur_job.job_id]'''
        if value > new_tst:
            new_tst = value
    if new_tst < cur_gpu.ava:
        new_tst = cur_gpu.ava
        print("cur_gpu.ava:",new_tst)
    tst[cur_gpu.gpu_id][cur_job.job_id] = new_tst
    print("tst:",tst[cur_gpu.gpu_id][cur_job.job_id])
    #print(tst[3][1])


def cal_tft(cur_job: job, cur_gpu: gpu, tst: List[List[float]], tft: List[List[float]],
            training_data: List[List[float]]):
    tft[cur_gpu.gpu_id][cur_job.job_id] = tst[cur_gpu.gpu_id][cur_job.job_id] + training_data[cur_job.job_id][cur_gpu.gpu_id]
    #print(tft[0][1])

def can_dedicate(cur_job: job, pre_job: job, cur_gpu: gpu, slot: List[List[float]], tst: List[List[float]],
                 tft: List[List[float]], training_data: List[List[float]], dag: DAG):
    #cal_tst(pre_job, cur_gpu, tst, tft, dag, training_data)
    #cal_tft(pre_job, cur_gpu, tst, tft, training_data)
    result = True
    if slot[cur_gpu.gpu_id][cur_job.job_id] < training_data[pre_job.job_id][cur_gpu.gpu_id]:
        #if cur_gpu.gpu_id == 0 and cur_job.job_id == 3:
            #print("condition 1")
        result = False
    if tft[cur_gpu.gpu_id][pre_job.job_id] > tst[cur_gpu.gpu_id][cur_job.job_id] or tft[cur_gpu.gpu_id][
        pre_job.job_id] == tst[cur_gpu.gpu_id][cur_job.job_id]:
        #if cur_gpu.gpu_id == 0 and cur_job.job_id == 3:
            #print("condition 2")
            #print(tft[cur_gpu.gpu_id][pre_job.job_id])
            #print(tst[cur_gpu.gpu_id][cur_job.job_id])
        result = False
    if pre_job in cur_gpu.job_list:
        #if cur_gpu.gpu_id == 0 and cur_job.job_id == 3:
            #print("condition 3")
        result = False
    return result


def insert_slot(cur_gpu: gpu, pred_job: job, cur_job: job, slot: List[List[float]], tst: List[List[float]],
                tft: List[List[float]], dag: DAG, training_data: List[List[float]]):
    pred_job.ass_gpu = cur_gpu
    cur_gpu.job_list.append(pred_job)
    print("[[[cal_pred_job_tst]]]")
    cal_tst(pred_job,cur_gpu,tst,tft,dag,training_data)
    print("[[[cal_pred_job_tst]]]")
    cal_tft(pred_job,cur_gpu,tst,tft,training_data)
    print("tft[cur_gpu.gpu_id][pred_job.job_id]:",tft[cur_gpu.gpu_id][pred_job.job_id])
    cur_gpu.ava = tft[cur_gpu.gpu_id][pred_job.job_id]

    '''print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    for JOB in cur_gpu.job_list:
        print(JOB.job_id,end=" ")
    print()'''
    #print("insert_slot----------------------------------------------")
    cal_tst(cur_job, cur_gpu, tst, tft, dag, training_data)

    #print("tst["+str(cur_gpu.gpu_id)+"]["+str(cur_job.job_id)+"]:"+str(tst[cur_gpu.gpu_id][cur_job.job_id]))
    #print("insert_finish---------------------------------------------")
    # cal_tft
    # cal_tft(cur_job, cur, tst, tft, training_data)
    # cal_slot
    slot[cur_gpu.gpu_id][cur_job.job_id] = tst[cur_gpu.gpu_id][cur_job.job_id] - cur_gpu.ava


def cal_total_time(dag: DAG, tft: List[List[float]]):
    result = 0
    for exit_job in dag.exits:
        if tft[exit_job.ass_gpu.gpu_id][exit_job.job_id] > result:
            result = tft[exit_job.ass_gpu.gpu_id][exit_job.job_id]
    return result
