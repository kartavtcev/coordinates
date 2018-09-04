# coordinates

### How to run this app with data
- Replace reduced.csv empty placeholder with real data.
- Run app with console arguments. I used IntelliJ IDEA.

##

Take-home Test test says: “This exercise should take you no more than two days.”.  
Usually I spent at least first half the day thinking about design decisions of the future solution (not jumping straight to write code), as it’s (arguably) most crucial phase & recommended by many experienced engineers/architects. 

Distributed Big Data platforms (Spark, Flink) don't look like the best fit for purposes of local demo.

### This exercise is my first Monix trial
Monix is fast implementation of [reactive streams](http://www.reactive-streams.org/).   
with Task (similar/compatible to IO from Cats) - deferred execution, effects (Alex Nedelcu, creator of Monix, contributes to Cats lib as well).  
Popular Reactive Extensions approach.  
Monix implements interface that supports pure-function programming & type-safety.

### Algorithm design
I propose an approximate solution to the problem, mostly to increase performance;  
anyway human intellect is only a limited-ratio model of an outside world.  

The first decision point is what is a meet(ing)/seen.  

In the “Software Engineer Take-home Test” doc, the term “met/meeting” is found 4 times, the term “seen” occurs once.   
So, what does it mean two people have met (may be shortly)?  

Actually my first idea was to solve movement [path intervals intersection problem](https://stackoverflow.com/questions/563198/how-do-you-detect-where-two-line-segments-intersect).

But there are a few issues with it / points on it:
- It suits best for transportation cases - where points move on large distances, and rarely intersected. (There could be optimizations on skipping points check with large distances, or Bounding Boxes check)
- In the office people move with max speed of 5 km/h = 1.4 m/s, & if people have met (incl. shortly), it does not guarantee their paths have intersected (on the same floor & within the same/similar time). 
So, lets approximately say, that 2 people have met if they spent 10-30 seconds on the distance less or equal to 5 metres  (on the same floor & in within the same/similar time).
- While I do real-time streaming approach, I calculate coordinates averages per traditional Minutes & process them per-Hour.   
To solve incoming old-data asynchronous problem (or sparseness of the events) (when we’re on the 3rd minute with Id #1 & we got data on the 1st minute from Id #2) I wait for 10 minutes shift, before processing the whole Hour coordinates data.
- Since my smallest approximation time is 1 minute, I’d work with an average of coordinates for each minute.

### Design Rationale
I decided to move with streaming solution, instead of giving thoughts on  
“What changes are necessary to support either … an infinite stream of input” - I converted file to stream.  
A large batch could be streamed.  

Performance of the algorithm, i.e., space and time complexity:  
- Time complexity: after incoming data is aggregated by minute & hour per Id, it's processed through a pipeline of algorithms:  
such as sorts (which should be O(nln(n))), 
sparseness calculation would have a binary search time complexity O(ln(n)) * (multiplied by) filter of whole of 2 Lists & sort inside each recursively-declining range, 
distance calculation should be linear in time, since it consists of a few O(1) per-coordinate operations.
- Space complexity: storing 2 hours of minute data per Id in Processor.scala, Algorithm.scala; storing meetups list.  
Sparseness calculation would use O(ln(n)) * n space, as it would store in v1,v2 whole List per each sum of recursive stacks/calls of the same level.


### Functional Programming thoughts on purity
My design moved from pure FP to KISS principle.

Yet, Processor.scala has one Task[MVar[F]] (Monix) to allow concurrent synchronization & mutual exclusion (replace var).  
(Cats has similar MVar[F[_], A]; and Ref[F[_], A])
```
val meetups: MVar[List[Meet]]
```  
  

```
private val ids: Array[Option[PerId]] = Array(None, None)
TODO: replace this mutable Array (vars) with MVar. Less critical than meetups MVar, since
1. it's private
2. because I use Monix Synchronous Subscriber & subscribed to single Observable (i.e. single thread)
```

TODO: DI with CAKE pattern?