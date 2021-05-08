import random
from random import randint as rd
import scipy.stats as stats
import numpy as np


def datamake(subjob_num: int, gpu_num: int):
    trainingdata = np.zeros((subjob_num, gpu_num))
    for i in range(subjob_num):
        a = random.choice(range(500, 3000))
        b = a * 2
        mu = (a + b) / 2
        sigma = random.choice(range(int(a/5), int(a/2)))
        dist = stats.truncnorm((a - mu) / sigma, (b - mu) / sigma, loc=mu, scale=sigma)
        values = dist.rvs(gpu_num)
        for j in range(gpu_num):
            trainingdata[i][j] = values[j]
        np.savetxt("trainingdata.txt", trainingdata, fmt='%.2f')
    return trainingdata


def DAGMake(job_num : int):
    #n = int(input("请输入设置的节点数："))
    node = range(1, job_num + 1)
    node = list(node)
    m = rd(job_num - 1, (job_num * (job_num - 1)) / 2 + 1)
    DAG = np.zeros((job_num, job_num))
    for i in range(0, m):
        p1 = rd(1, job_num - 1)
        p2 = rd(p1 + 1, job_num)
        x = node[p1 - 1]
        y = node[p2 - 1]
        l = np.random.randint(500, 4000)
        if DAG[x - 1][y - 1] != 0:
            continue
        else:
            DAG[x - 1][y - 1] = l
    print(DAG)
    np.savetxt("DAG.txt", DAG, fmt='%.2f')
    return DAG

