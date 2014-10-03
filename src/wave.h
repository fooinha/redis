/* wave.h - Basic wave implementation
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

#ifndef __WAVE_H__
#define __WAVE_H__

#include "redisassert.h"
#include "adlist.h"
#include <math.h>


/// LLONG_MIN   : -9223372036854775808
/// LLONG_MAX   :  9223372036854775807

/// R ε ]LLONG_MIN, LLONG_MAX[
/// The sum over a sliding window can range from 0 to NR.
/* Calculates the max increment value */
inline long long waveMaxIncrement(long long N) {

    redisAssert(N > 0);

    return floor(LLONG_MAX)/N;
}

/// Let N’ be smallest power of 2 greater than/equal to 2RN.
inline long long waveModulo(long long N, long long R) {

    /* Try to detect overflow */
    if ( ( R > 0 && (N > ( LLONG_MAX / (2*R) ))) )
        return LLONG_MAX; //TODO: maybe 2^62 is safer

    unsigned i =0;
    long long ret = 0;
    for (; i < 63; ++i) {
        ret = pow(2,i);
        if (ret >= (2*N*R))
            break;
    }

    return ret;
}

#define WAVE_MODULO_N(v,M) (v%M)
#define WAVE_MODULO_OBJ(v,w) (v%w->M)

///
/// We maintain two modulo N’ counters:
///        pos , the current length,
///  and total , the running sum.

/// Counters(modulo N’):
/// pos  - the current length
/// total - the running sum
/// l=log(2ENR) levels.

typedef struct wave {
    char expire;        // Wave auto expires                Default: YES
    long long N;        // Wave "window" size               Default: 60
    double E;           // Relative error rate              Default: 0.05
    long long R;        // Wave increment domain            Default: 1024
    long long M;        // Wave modulo                      waveModulo(N,R)

    long long start;    // Start timestamp                  now()
    long long last;     // Last timestamp                   now()
    long long pos;      // The current length               %(now()-start,N)
    long long total;    // The running sum.                 [0,NR]

    long long z;        // Last discarded total("count")    [0,NR]
    list **l;           // List of levels queues
    list *L;            // Sorted linked list for (pos,rank)
} wave;

/// Storing triple for each item (p,v,z)
/// v-the value for the data item
/// z-the partial sum trough this item
typedef struct waveitem {
   long long pos;       // the current position
   long long v;         // increment value
   long long z;         // partial sum
} waveitem;


/* Calculates the number of "levels" for the wave
 * The wave contains the positions of the sum values,
 * arranged at different “levels.”
 * There are l = log 2 (2εNR ) levels, numbered 0 to l − 1.
 */
inline long long waveNumLevels(wave * w) {

    int base = 2;

    redisAssert(w);

    long long N = w->N;
    double E = w->E;
    long long R = w->R;

    redisAssert(R > 0);

    long long r = R ;

    if (! r)
        r = waveMaxIncrement(N);
    //else
    //FIXME

    float L = log(2*E*N*r)/log(base);


    long long F = abs(ceil(L));

//    printf("waveNumLevels: 2*E*N*r=%f L=%f r=%lld F=%lld\n",
//           2*E*N*r, L, r, F);

    if (F>62)
        return 63;

    return 1+F;
}


waveitem *waveitemCreate(long long pos, long long v, long long z);
wave *waveCreate(long long N, double E, long long R, long long ts, char expire);

#endif /* __WAVE_H__ */
