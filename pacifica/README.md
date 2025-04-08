# 草稿

1. 算法：pacifica
2. 存储
    - 持久化 []entry
    - 易失性 commitIndex(节点重启后尝试append最后一条已存储日志来恢复)，lastApplied,currentTerm,next_index,status,一系列配置中心的配置
    - snapshot作为接口，由外部实现
    - leader易失性 nextIndex Map[],matchIndex Map[]+最小堆
    - follower易失性 leader_lastindex
3. 配置中心（etcd）
    - Root：/Pacifica/Group${number} 或 /Pacifica
    - followers：.../followers/${address}
    - leader：.../leader : ${address}
    - version：.../version : ${number}
4. 状态变化
    - leader->learner
    - follower->leader
    - follower->learner
    - learner->follower

## 优化：流水线、租期读...