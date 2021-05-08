from typing import List
from entity import job,gpu,DAG


def original_allocation(job_list: List[job], gpu_list: List[gpu], trainingdata:List[List[float]],dag:DAG):
    tst = [0]*len(job_list)
    tft = [0]*len(job_list)
    index = 0
    for JOB in job_list:
        #print("JOB:",JOB.job_id)
        JOB.ass_gpu = gpu_list[index % len(gpu_list)]
        gpu_list[index % len(gpu_list)].job_list.append(JOB)
        index += 1
        if len(JOB.pred_jobs) == 0:
            tst[JOB.job_id] = gpu_list[index % len(gpu_list)].ava#assume that the number of enters is smaller than the number of gpu_resource
        else:
            pred_value = []
            for pred_job in JOB.pred_jobs:
                edge_weight = 0
                if pred_job.ass_gpu != JOB.ass_gpu:
                    edge_weight = dag.DAG_detail[pred_job.job_id][JOB.job_id]
                pred_value.append(edge_weight+tft[pred_job.job_id])
            tst[JOB.job_id] = max(pred_value)
        #print("pred_value:",pred_value)
        #print("tst:",tst[JOB.job_id])
        tft[JOB.job_id] = tst[JOB.job_id] + trainingdata[JOB.job_id][gpu_list[index % len(gpu_list)].gpu_id]
        #print("training_time:",trainingdata[JOB.job_id][index % len(gpu_list)])
        #print("tft:",tft[JOB.job_id])
        gpu_list[index % len(gpu_list)].ava = tft[JOB.job_id]
    return max(tft)
