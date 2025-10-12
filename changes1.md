okay, so I need to make some changes

currently we have MmapMut, we use this stuff with it:

.write(offset,buffer)
.read(offset,buffer)
.clone()

I want to make an fd based abstraction which handles those apis so that we don't have to make changes in a lot of places

so currently we have:

```
struct SharedMmap {
    mmap: MmapMut,
    last_touched_at: AtomicU64,
}
```

I want to use flags such that:

like the mmap pool thingy too

how about we just... yeah, just use a darn <T> thingy lmao, if we send a flag use FDs, if we don't send a flag use mmap
this can replace

so just a <T> which can either be MmapMut or our FDstuff and for now, let's just let the flag be a global thingy
this is way more doable

just make this and change the behaviour of new() in there, yep, works

also kinda... just check, if we init with SyncEach fsyncschedule, we want to open the fd in `O_SYNC` mode

so overall, just change with our custom <T> thingy wherever we usd MmapMut, that's it

and have some sort of abstraction for the fs stuff such that supports all the above mentioned MmapMut apis we use right now