pure async replication is annoying/dumb/stupid, cant do the fan out part fast as the second order fan ins can kill the leader, need to do it slowly in follower batches, which leads to blocking on the leader's side and kills throughput and makes replication slow

not adding partitioning for now, forget it, not happening in the moment, too complex