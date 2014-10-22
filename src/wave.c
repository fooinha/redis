/* wave.h - Basic deterministic wave implementation
 *          * With sum of bounded integers variation *
 *
 * Copyright (c) 2014, Paulo Pacheco <fooinha at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


///  3.3.Sum of Bounded Integers
///
///  The deterministic wave scheme can be extended to handle the problem of maintaining
///the sum of the last N items in a data stream, where each item is an integer in [0..R].
///
///
/// Distributed Streams Algorithms for Sliding Windows et al.
/// Phillip B. Gibbons [1] and Srikanta Tirthapura [2]


#include "redis.h"
#include "wave.h"



/* Creates a wave item to place in wave's levels queues */
waveitem *waveitemCreate(long long pos, long long v, long long z) {

   struct waveitem *i;

   if ((i = zmalloc(sizeof(*i))) == NULL)
      return NULL;

   i->pos = pos;
   i->v = v;
   i->z = z;

   return i;
}

/* A level is full if it has ceil(1/ε + 1) positions. */
inline unsigned long waveLevelMaxPositions(double E) {

   if (! E)
      E = 0.01;

   return ceil( (1 / E )  + 1 );
}

/* Creates a wave to place in database */
wave *waveCreate(long long N, double E, long long R, long long ts, char expire) {

   struct wave *wave;

   if ((wave = zmalloc(sizeof(*wave))) == NULL)
      return NULL;

   if (! E)
      E = 0.05;

   if (! ts)
      ts = time(0);

   wave->expire = expire;
   wave->N = N;
   wave->E = E;
   wave->R = R;
   wave->M = waveModulo(N,R);
   wave->start = ts;
   wave->last = ts;
   wave->pos = 0;
   wave->total = 0;
   wave->z = 0;

   long long num_levels = waveNumLevels(wave);

//   printf("N=%lld E=%f R=%lld #L=%lld M=%lld\n",N, E, R, num_levels, waveModulo(N, R));


   // Wave queue levels
   wave->l = zmalloc(sizeof(*(wave->l))*num_levels);

   /// TODO: Lazy creation ???
   for (unsigned int n = 0; n < num_levels; ++n) {
      wave->l[n] = listCreate();
   }

   // Linked List L
   wave->L = listCreate(); ///TODO: catch oom handler / abort?

   return wave;
}


void wavePurgeLists(wave *w) {

   long long num_levels = waveNumLevels(w);

   /* Purge every wave level queue l[] */
   for  (int j = 0; j < num_levels; ++j) {

      if (! w->l[j])
         continue;

      listNode *jn;
      listIter ji;
      listRewind(w->l[j],&ji);

      while((jn = listNext(&ji))) {
         listDelNode(w->l[j], jn);
      }
   }

   /* Purge linked list L */
   if (w->L) {
      listNode *kn;
      listIter ki;

      listRewind(w->L,&ki);
      while((kn = listNext(&ki))) {
         listDelNode(w->L, kn);
      }
   }

}

/* Resizes wave's levels and linked list and updates size control numbers */
void waveResize(wave *w, long long N, double E, long long R) {

   wavePurgeLists(w);

   w->N = N;
   w->E = E;
   w->R = R;
   w->M = waveModulo(N,R);

}


/* Resets wave to zero and sets position to current ts */
void waveReset(wave* w) {

   w->start = time(0);
   w->last = time(0);
   w->pos = 0;
   w->total = 0;
   w->z = 0;

   wavePurgeLists(w);
}

void wavePrint(wave *w) {


   printf(" C -----------------------------------------------\n");
   printf("\t   EXP => %d\n", w->expire);
   printf("\t     N => %lld\n", w->N);
   printf("\t     E => %f\n", w->E);
   printf("\t     R => %lld\n", w->R);
   printf("\t start => %lld\n", w->start);
   printf("\t  last => %lld\n", w->last);
   printf("\t   pos => %lld\n", w->pos);
   printf("\t  rank => %lld\n", w->total);
   printf("\t     r => %lld\n", w->z);
   printf(" C -----------------------------------------------\n\n");

   long long num_levels = waveNumLevels(w);

   printf(" W -----------------------------------------------\n");
   printf("\t    #l => %lld\n", num_levels);
   printf("\tmax sz => %lu\n", waveLevelMaxPositions(w->E));

   long long j;
   for ( j = 0 ; j < num_levels ; ++j) {

      if (! listLength(w->l[j]))
         continue;

      listNode *jn;
      listIter ji;
      listRewind(w->l[j],&ji);

      while((jn = listNext(&ji))) {
         waveitem *item = jn->value;
         printf("\t   l[%lld] => ( pos=%lld , v=%lld , z=%lld ) \n", j, item->pos, item->v, item->z );

      }
      printf("\n");
   }
   printf(" W -----------------------------------------------\n\n");
   printf(" L -----------------------------------------------\n");
   printf("\t  sz L => %lu\n", listLength(w->L));

   listNode *kn;
   listIter ki;
   listRewind(w->L,&ki);
   while((kn = listNext(&ki))) {
      waveitem *item = kn->value;
      printf("\t  ( pos=%lld , v=%lld , z=%lld ) \n", item->pos, item->v, item->z );
   }

   printf(" L -----------------------------------------------\n");

}


/// When answering a query, we know that
/// the window sum is in [ total − z 2 + v 2 , total − z 1 ], where ( p, v 2 , z 2 ) is the triple at the
/// head of the linked list L and z 1 is the largest partial sum discarded, and we return the
/// midpoint of this interval.

/// 1. If N ≥ pos, return x ˆ := total as the exact answer. Otherwise, let z 1 be the largest partial
/// sum discarded from L (or 0 if no partial sum has been discarded). Let ( p, v 2 , z 2 ) be the
/// triple at the head of the linked list L. (If L is empty, return x ˆ := 0 as the exact answer.)
/// 2. If p = pos − N + 1, return x ˆ := total − z 2 + v 2 as the exact answer. Otherwise, return
/// x ˆ := total − (z 1 + z 2 − v 2 )/2.
///
/// Some adaptations ...

long long waveGet(wave *w, long long ts, char fast_total) {

   if (! w) return 0;
   if (! ts) return 0;


   /* Too old for this sliding window */
   if (ts < w->start  )
      return 0;

   /* Too old for this sliding window */
   if (ts <= (w->last - w->N)) {
      //printf("EXACT #1.1 - TS = %lld\n",ts);
      return 0;
   }

   /* Too recent for this sliding window */
   if (ts >= (w->last + w->N)) {
      //printf("EXACT #1.2 - TS = %lld\n",ts);
      return 0;
   }

   /// 0. If N is equal to last
   if (ts == w->last) {
       //printf("EXACT #0 - TS = %lld\n",ts);
       return (w->total-w->z);
   }

   /// 1. If N ≥ pos, return x ˆ := total as the exact answer.
   /// Not suitable
//   if (ts > w->last && (ts <= (w->last + w->N))) {
//      printf("EXACT #1 - TS = %lld\n",ts);
//      return w->total;
//   }


   /// (If L is empty, return x ˆ := 0 as the exact answer.)
   if (listLength(w->L) == 0) {
      //printf("EXACT #2 - TS = %lld\n",ts);
      return 0;
   }

   long long total = w->total;

   listNode *h = listFirst(w->L);
   waveitem *head = h->value;

   /* Walks head to infimum value for given ts */
   while (head && head->pos < ( ts - w->N) ) {
      h = h->next;

      if (h && h->value)
         head = h->value;
      else 
         break;
   }


   ///  let z 1 be the largest partial sum discarded from L
   long long z1 = w->z;

   ///  Let ( p, v 2 , z 2 ) be the triple at the head of the linked list L
   long long p = head->pos;
   long long v2 = head->v;
   long long z2 = head->z;


   /// 2. If p = pos − N + 1, return x ˆ := total − z 2 + v 2 as the exact answer.

   if (p == (ts - w->N + 1)) {
      //printf("EXACT #4 - TS = %lld z2=%lld v2=%lld\n",ts,z2,v2);
      return (w->total - z2 +v2);
   }

   if (p == (ts - w->N)) {
      //printf("EXACT #5 - TS = %lld z2=%lld\n",ts,z2);
      return(w->total - z2);
   }

   if (ts == w->pos) {
      //printf("EXACT #6 - TS = %lld\n",ts);
      return(w->total - w->z);
   }

   /* FAST ESTIMATIVE */
   if (fast_total) {
       return ( total - floor((z1 + z2 - v2)/(2)) );
   }


   // Adaption - Transverse Linked List L to calculate the correct total and not an estimative value

   //printf(" ? TS = %lld => P = %lld\n", ts, p);
   ///wavePrint(w);

   /* In the past */
   //TODO: to re-test this
   if (ts < w->last) {

      long long future_total = 0;

      listNode *kn;
      listIter iter;
      listRewindTail(w->L,&iter);

      while((kn = listNext(&iter))) {
         waveitem *item = kn->value;

         if (item->pos <= WAVE_MODULO_OBJ(ts - w->start,w))
            future_total += item->v;

         //printf(" : TS=%lld I:POS=%lld I:V=%lld FUTURE_TOTAL:%lld\n", ts, item->pos, item->v, future_total);

      }

      //printf(" $ TS=%lld POS=%lld TOTAL = %lld => FUTURE_TOTAL = %lld\n", ts, w->pos , total, future_total);

      total -= future_total;

      //printf("SUM #8 - TS = %lld TOTAL=%lld z1=%lld z2=%lld v2=%lld\n", ts, w->total, z1, z2, v2);
      //return ( total - floor((z1 + z2 - v2)/(2)) );
      return total;
   }


   /* In the future */
   long long win_total = 0;

   listNode *kn;
   listIter iter;
   listRewind(w->L,&iter);

   while((kn = listNext(&iter))) {
      waveitem *item = kn->value;

      long long p = item->pos; // current position
      long long l = WAVE_MODULO_OBJ(ts - w->start - w->N, w); // sliding window limit for given ts

      //printf("* ADD?: p=%lld l=%lld ip=%lld v=%lld\n", p, l, item->pos, item->v);


        if (p > l) {
            //printf("* ADDED: p=%lld l=%lld v=%lld\n", p, l, item->v);
            win_total += item->v;
        }
   }


   //printf("SUM #9 - TS = %lld WIN_TOTAL=%lld \n", ts, win_total);

   return win_total;

}


///The challenge is to compute the wave level in step 3(a) quickly; we show how to
///do this in O(1) time. First observe that the desired wave level is the largest position j
///(numbering from 0) such that some number y in the interval ( total , total + v] has 0’s in all
///bit positions less than j (and hence is a multiple of 2^j ). Second, observe that y − 1 and y
///differ in bit position j, and if this bit changes from 1 to 0 at any point in [ total , total + v],
///then j is not the largest. Thus, j is the position of the most-significant bit that is 0 in
///total and 1 in total + v.
///
/// Accordingly, let f be the bitwise complement of total , and let
///g = total + v. Let h = f ∧ g, the bitwise AND of f and g. Then the desired wave level
///is the position of the most-significant 1-bit in h, i.e., floor(log h) .


unsigned long waveComputeTotalLevel(
      long long total,
      long long v,
      unsigned long num_levels) {

   unsigned long j,f,g,h;

   if (num_levels <=1 )
      return 0;

   j = num_levels - 1;


   /* let f be the bitwise complement of total */
   f = ~total  ;

   /* let g = total + v */
   g = ~(total + v);

   /* Let h = f ∧ g, the bitwise AND of f and g */
   h = (f ^ g) ;

   /* Then the desired wave level is the position of the
    * most-significant 1-bit in h, i.e., floor(log h) .
    */

   j = floor(log(h)) ;

   // printf("|=> TOTAL=%lld V=%lld J=%lu L=%lu LOG(H)=%f\n", total, v, j, num_levels, log(h) );

   if (j >= num_levels)
      j = num_levels - 1;

   // printf("|=> F=%lu, G=%lu, H=%lu, J=%lu \n", f, g, h, j );

   return j;
}


/// Upon receiving an item with value v ∈ [0..R]:
///
///
/// 1. Increment pos. // All additions and comparisons are done modulo N
/// 2. If the head ( p, v , z) of the linked list L has expired (i.e., p ≤ pos − N ), then discard it
/// from L and from (the tail of) its queue, and store z as the largest partial sum discarded.
/// 3. If v > 0, then do:
/// (a) Determine the wave level, i.e., the largest j such that some number in (total, total + v)
/// is a multiple of 2 j (waveComputeTotalLevel) . Add v to total.
/// (b) If the level j queue is full, then discard the tail of the queue and splice it out of L.
/// (c) Add (pos, v, total) to the head of the level j queue and the tail of L.
///
int waveSet(wave *w, long long v, long long ts) {

   if (! w) return REDIS_ERR;
   if (v <= 0) return REDIS_ERR;
   if (! ts) return REDIS_ERR;


   /* Too old for this sliding window */
   if (ts < w->start )
      return REDIS_OK;

   long long num_levels = waveNumLevels(w);

   /// 1. Increment pos  ... update pos if it's more recent

   /* Algorithm adaptation for timestamps */
   if (ts > w->start && ts > w->last) {
      w->pos = WAVE_MODULO_N((ts - w->start), w->M);
      w->last = ts;
   }

   /// 2. If the head ( p, v , z) of the linked list L has expired (i.e., p ≤ pos − N ), then discard it
   /// from L and from (the tail of) its queue, and store z as the largest partial sum discarded.

   listNode *head = listFirst(w->L);

   ///TODO: The documentation implies a loop or only a single removal???
   int fast = 0;

   if (head) {

      waveitem *item = (waveitem *) head->value;


       //printf("**** ITEM->P=%lld (w->POS-W->N)=%lld\n", item->pos, (w->pos - w->N ));

      /* If has expired -  (i.e., p ≤ pos − N ) */
      ///TODO: The documentation implies a loop or only a single removal???
      while (head && item &&
                item->pos <= (w->pos - w->N  )
             ) {

         /* store z as the largest partial sum discarded. */
         w->z = item->z;

         //printf("**** ITEM->P=%lld (w->POS-W->N)=%lld\n", item->pos, (w->pos - w->N ));
         //printf("**** POS=%lld Z-DISCARDED=%lld \n", item->pos, item->z);
         //printf("-----------------------------------------\n");

         /*  discard it from L and from (the tail of) its queue */
         long long j = waveComputeTotalLevel(item->z, 0, num_levels);

         listNode *jn;
         listIter ji;
         listRewind(w->l[j],&ji);

         while((jn = listNext(&ji))) {

            waveitem *wj = jn->value;
            if ((wj->pos == item->pos) && (wj->v == item->v) && (wj->z == item->z)) {

               //printf("**** WAVE J=%lld, POS=%lld Z-DISCARDED=%lld \n", j, wj->pos, wj->z);
               listDelNode(w->l[j], jn);
               break;
            }
         }

         listDelNode(w->L, head);

         ///TODO: The documentation implies a loop or only a single removal???
         if (fast)
            break;

         head = listFirst(w->L);
         if (! head)
            break;

         item = (waveitem *) head->value;

      }
   }

   ///3.(a) Determine the wave level, i.e., the largest j such that some number in (total, total + v)
   ///is a multiple of 2^j .
   ///The challenge is to compute the wave level in step 3(a) quickly

   long long j = waveComputeTotalLevel(w->total, v, num_levels);

   /// 3.(a) Add v to total.
   w->total += v;


   /// (b) If the level j queue is full, then discard the tail of the queue and splice it out of L.
   if (w->l && w->l[j] && 
         listLength(w->l[j]) > waveLevelMaxPositions(w->E)) {

      listNode *kn;
      listIter ki;

      listNode *tail = listLast(w->l[j]);
      waveitem *tail_item = tail->value;

      /* splice it out of L */
      listRewind(w->L,&ki);
      while((kn = listNext(&ki))) {
         waveitem *wk = kn->value;

         if (
               (tail_item->v == wk->v) &&
               (tail_item->pos == wk->pos) &&
               (tail_item->z == wk->z)

            ){
            listDelNode(w->L, kn);
            break;
         }
      }

      /* discard the tail of the queue */
      listDelNode(w->l[j], tail);
   }

   /// (c) Add (pos, v, total) to the head of the level j queue and the tail of L.
   waveitem * new = waveitemCreate(w->pos, v, WAVE_MODULO_N(w->total,w->M));
   if (new == NULL) return REDIS_ERR;

   //    printf(" * ADD TS=%lld V=%lld Z=%lld\n", ts, v, w->total);

   if (w->l && w->l[j])
      listAddNodeHead(w->l[j], new);  // Add(pos,rank) to the head of the level j

   if (w->L)
      listAddNodeTail(w->L, new);     // and the tail of L


   return REDIS_OK;
}

int waveObjectFromDB(redisClient *c, robj **out) {


   robj *o = NULL;

   o = lookupKeyWrite(c->db, c->argv[1]);

   if (o == NULL) {
      *out = NULL;
      return REDIS_OK;
   }

   if (o->type != REDIS_WAVE) {

      addReply(c, shared.wrongtypeerr);
      return REDIS_ERR;

   } else  {
      *out = o;
      return REDIS_OK;

   }
   addReply(c, shared.err);
   return REDIS_ERR;

}


robj *createWaveObject(long long N, double E, long long R, long long ts, char expire) {
   wave *w = waveCreate(N, E, R, ts, expire);
   robj *o = createObject(REDIS_WAVE,w);
   o->encoding = REDIS_ENCODING_RAW;
   return o;
}

void freeWaveObject(robj *o) {
   wave *w = (wave *) o->ptr;

   long long num_levels = waveNumLevels(w);

   for (unsigned int n = 0; n < num_levels; ++n) {
      listRelease(w->l[n]);
   }

   listRelease(w->L);
   zfree(o->ptr);
}


/* Commands */

/* WVRESET key [key ...] */
void wvresetCommand(redisClient *c) {

   int done = 0, j;

   for (j = 1; j < c->argc; j++) {

      robj * o = lookupKeyWrite(c->db, c->argv[j]);
      if (! o)
         continue;

      waveReset(o->ptr);
      done++;
   }

   addReplyLongLong(c,done);
}

/* WVDEBUG key [SHOW-LISTS=yes]*/
void wvdebugCommand(redisClient *c) {

   robj *o;
   char show_lists = 0;

   if (c->argc > 3) {
      addReply(c, shared.syntaxerr);
      return;
   }


   if (c->argc == 3) {
      /* Get argument flag for auto expire */
      if (equalStringObjects(shared.yes, c->argv[2]))
         show_lists = 1;
   }

   if ( waveObjectFromDB(c, &o) != REDIS_OK) {
      return;
   }

   if (o == NULL) {
      addReply(c, shared.nokeyerr);
      return;
   }

   wave *w = o->ptr;

   if (w == NULL) {
      addReply(c, shared.oomerr);
      return;
   }

   list * lines = listCreate();

   listAddNodeTail(lines, sdscatprintf(sdsempty()," C -----------------------------------------------"));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"     NOW => %ld", time(0)));


   listAddNodeTail(lines, sdscatprintf(sdsempty()," C -----------------------------------------------"));

   listAddNodeTail(lines, sdscatprintf(sdsempty(),"  EXPIRE => %d", w->expire));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"       N => %lld", w->N));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"       E => %f", w->E));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"       R => %lld", w->R));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"       M => %lld", w->M));

   listAddNodeTail(lines, sdscatprintf(sdsempty()," C -----------------------------------------------"));

   listAddNodeTail(lines, sdscatprintf(sdsempty(),"start ts => %lld", w->start));
   listAddNodeTail(lines, sdscatprintf(sdsempty()," last ts => %lld", w->last));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"limit ts => %lld", w->last + w->N));

   listAddNodeTail(lines, sdscatprintf(sdsempty(),"     pos => %lld", w->pos));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"   total => %lld", w->total));
   listAddNodeTail(lines, sdscatprintf(sdsempty(),"       z => %lld", w->z));

   listAddNodeTail(lines, sdscatprintf(sdsempty()," C -----------------------------------------------"));

   listNode *kn;
   listIter ki;

   if (show_lists) {

      long long num_levels = waveNumLevels(w);
      listAddNodeTail(lines, sdscatprintf(sdsempty()," "));

      listAddNodeTail(lines, sdscatprintf(sdsempty()," W -----------------------------------------------"));
      listAddNodeTail(lines, sdscatprintf(sdsempty(),"    #l => %lld", num_levels));
      listAddNodeTail(lines, sdscatprintf(sdsempty(),"max sz => %lu", waveLevelMaxPositions(w->E)));

      long long j;
      for ( j = 0 ; j < num_levels ; ++j) {

         if (! listLength(w->l[j]))
            continue;

         unsigned int k = 0;
         listNode *jn;
         listIter ji;
         listRewind(w->l[j],&ji);

         listAddNodeTail(lines, sdscatprintf(sdsempty()," l [%lld] --------------------------------------------", j));
         while((jn = listNext(&ji))) {
            waveitem *item = jn->value;
            listAddNodeTail(lines, sdscatprintf(sdsempty(),"      [%2u] => ( p=%lld , v=%lld , z=%lld )", k, item->pos, item->v, item->z ));
            ++k;
         }
         listAddNodeTail(lines, sdscatprintf(sdsempty()," l -----------------------------------------------"));

      }
      listAddNodeTail(lines, sdscatprintf(sdsempty()," "));

      listAddNodeTail(lines, sdscatprintf(sdsempty()," W -----------------------------------------------"));
      listAddNodeTail(lines, sdscatprintf(sdsempty()," L -----------------------------------------------"));
      listAddNodeTail(lines, sdscatprintf(sdsempty(),"  sz L => %lu", listLength(w->L)));

      listRewind(w->L,&ki);
      while((kn = listNext(&ki))) {
         waveitem *item = kn->value;
         listAddNodeTail(lines, sdscatprintf(sdsempty(),"  ( p=%lld , v=%lld , z=%lld )", item->pos, item->v, item->z ));
      }

      listAddNodeTail(lines, sdscatprintf(sdsempty()," L -----------------------------------------------"));

   }

   addReplyMultiBulkLen(c, listLength(lines));

   listRewind(lines,&ki);
   while((kn = listNext(&ki))) {
      addReplyBulkCBuffer(c, kn->value, sdslen(kn->value));
   }

   listRelease(lines);

}

/* WVINCRBY key <incr=0> <timestamp=now>  [EXPIRE=yes]  <wave-N=60> <wave-E=0.05> <wave-R=1024>*/
///TODO: Missing refcount and notification.
void wvincrbyCommand(redisClient *c) {


   /* Too many arguments */
   if (c->argc > 8) {
      addReply(c, shared.syntaxerr);
      return;
   }

   /* simple tribool 2=> undefined */
   char expire = 2;

   double E = 0.05;
   long long N  = 60 , ts = 0, incr=1, R=-1;

   /* Get arguments */
   switch(c->argc) {

      case 8: {

                 static char *err_invalid_type_R = "value for R is not a valid long";
                 static char *err_invalid_value_R = "value for R must be bigger than 0";


                 if ( getLongLongFromObjectOrReply(c,c->argv[7],
                          &R, err_invalid_type_R) != REDIS_OK )
                    return;

                 /* R >= -1 */
                 if (R < -1) {
                    addReplyError(c,err_invalid_value_R);
                    return;
                 }

              }


      case 7:  {
                  /* Get argument value for relative E(error) */

                  static char *err_invalid_type_E = "value for E is not a valid float";
                  static char *err_invalid_value_E = "value for E must be between ]0,1[";

                  if ( getDoubleFromObjectOrReply(c,c->argv[6],
                           &E, err_invalid_type_E) != REDIS_OK )
                     return;

                  /*  0 < E < 1 */
                  if(E <= 0 ||  E>=1) {
                     addReplyError(c,err_invalid_value_E);
                     return;
                  }

               }

      case 6: {
                 /* Get argument value for wave size */

                 static char *err_invalid_type_N = "value for N is not a valid long";
                 static char *err_invalid_value_N = "value for N must be bigger than 0";


                 if ( getLongLongFromObjectOrReply(c,c->argv[5],
                          &N, err_invalid_type_N) != REDIS_OK )
                    return;

                 /* N >= -1 */
                 if (N < -1) {
                    addReplyError(c,err_invalid_value_N);
                    return;
                 }

              }
      case 5: {

                 /* Get argument flag for auto expire */
                 if (equalStringObjects(shared.no, c->argv[4]))
                    expire = 0;

                 if (equalStringObjects(shared.yes, c->argv[4]))
                    expire = 1;

              }
      case 4:{

                static char *err_invalid_type_ts = "value for ts is not a valid long";
                static char *err_invalid_value_ts = "value for ts must not be negative";

                if ( getLongLongFromObjectOrReply(c,c->argv[3],
                         &ts, err_invalid_type_ts) != REDIS_OK )
                   return;

                /* ts > 0 */
                if (ts < 0) {
                   addReplyError(c,err_invalid_value_ts);
                   return;
                }

             }

      case 3: {
                 static char *err_invalid_type_incr = "value for incr is not a valid long";
                 static char *err_invalid_value_incr = "value for incr must not be negative";

                 if ( getLongLongFromObjectOrReply(c,c->argv[2],
                          &incr, err_invalid_type_incr) != REDIS_OK )
                    return;

                 /* incr > 0 */
                 if (incr < 0) {
                    addReplyError(c,err_invalid_value_incr);
                    return;
                 }

              }

   }

   if (R == -1)
      R = floor(LLONG_MAX / N);



   /* server timestamp if not given as argument */
   if (ts == 0)
      ts = time(0);

   robj *o;

   if ( waveObjectFromDB(c, &o) != REDIS_OK) {
      return;
   }

   if (o == NULL) {

      o = createWaveObject(N, E, R, ts, expire);

      if (o == NULL) {
         addReply(c, shared.oomerr);
         return;
      }

      setKey(c->db,c->argv[1],o);
   }

   long long total = 0;

   wave *w = (wave *) o->ptr;

   /* Check if N and E should be changed */
   if ( ( c->argc == 6 && w->N != N ) ||
         ( c->argc == 7 && w->E != E ) ||
         ( c->argc == 8 && w->R != R )
      )
      waveResize(w, N, E, R);

   if (c->argc == 4 && expire != 2)
      w->expire = expire;

   /* Increment value cannot be bigger than R */
   if (incr > w->R) {
      addReply(c, shared.toobigerr);
      return;
   }

   if (incr > 0)  {
      if ( waveSet(w, incr, ts ) == REDIS_ERR) {
         addReply(c, shared.oomerr);
         return;
      }
   }

   //printf("MAX SUM=%lld\n", waveMaxIncrement(w->N));
   total = waveGet(w, ts, 0);

   /* If auto expire is on for this wave */
   if (w->expire) {

      /* Expire key from pos to N */
      long long when = (w->last + (( w->N) + 1 ));
      when *= 1000;
      setExpire(c->db,c->argv[1],when);
   }

   /* Send reply of total for this wave*/
   robj *ret = createStringObjectFromLongLong(total);

   if (! ret) {
      addReply(c, shared.oomerr);
      return;
   }

   addReply(c,shared.colon);
   addReply(c,ret);
   addReply(c,shared.crlf);

}

/* WVGET key <timestamp=now> [fast=no] */
void wvgetCommand(redisClient *c) {

   /* Too many arguments */
   if (c->argc > 4) {
      addReply(c, shared.syntaxerr);
      return;
   }

   robj *o;
   long long ts = 0;
   long long total = 0;
   char fast_total = 0;

   if ( waveObjectFromDB(c, &o) != REDIS_OK) {
      return;
   }

   if (! o) {
      addReply(c, shared.nokeyerr);
      return;
   }

   if (c->argc >= 3 ) {
      static char *err_invalid_type_ts = "value for ts is not a valid long";
      static char *err_invalid_value_ts = "value for ts must not be negative";

      if ( getLongLongFromObjectOrReply(c,c->argv[2],
               &ts, err_invalid_type_ts) != REDIS_OK )
         return;

      /* ts cannot be < 0 */
      if (ts < 0) {
         addReplyError(c,err_invalid_value_ts);
         return;
      }
   }

   if (c->argc >= 4) {

       /* Get argument flag for auto expire */
       if (equalStringObjects(shared.no, c->argv[3]))
          fast_total = 0;

       if (equalStringObjects(shared.yes, c->argv[3]))
          fast_total = 1;
   }



   if (! ts)
      ts = time(0);


   wave *w = (wave *) o->ptr;


   if (!w) {
      addReply(c, shared.err);
      return;
   }

   total = waveGet(w, ts, fast_total);

   /* Send reply of total for this wave*/
   robj *ret = createStringObjectFromLongLong(total);

   if (! ret) {
      addReply(c, shared.oomerr);
      return;
   }

   addReply(c,shared.colon);
   addReply(c,ret);
   addReply(c,shared.crlf);

}


/* WVTOTAL key  */
void wvtotalCommand(redisClient *c) {

   /* Too many arguments */
   if (c->argc > 2) {
      addReply(c, shared.syntaxerr);
      return;
   }

   robj *o;

   if ( waveObjectFromDB(c, &o) != REDIS_OK) {
      return;
   }

   if (! o) {
      addReply(c, shared.nokeyerr);
      return;
   }


   wave *w = (wave *) o->ptr;

   if (!w) {
      addReply(c, shared.err);
      return;
   }


   /* Send reply of total for this wave*/
   robj *ret = createStringObjectFromLongLong(w->total);

   if (! ret) {
      addReply(c, shared.oomerr);
      return;
   }

   addReply(c,shared.colon);
   addReply(c,ret);
   addReply(c,shared.crlf);

}


