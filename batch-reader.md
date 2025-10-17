we need to add a batch read endpoint just like batch write endpoint that we have

for some topic, external shits can ask to read X bytes from it, essentially we want to read a bunch of entries which can fit <= X since the last entry we read till then, 

for this, it seeems to be pretty easy,

we first figure out from the reader chain how many blocks would we need to read (note that blocks can be bigger than 10mb),
then use io_uring to read those shits, if we get any error to read any of it, we return instantly with failure,

note that entries need to be sent in the order they were written(because io_uring might return something out of order)

how would you do it minimally, does my idea looks good ? this is a zero cost abstraction right ? i.e. there is no better way anyone could do this batch read thingy right ?  
