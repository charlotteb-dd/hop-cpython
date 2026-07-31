/*! ******************************************************************************
 *
 * CPython for the Hop orchestration platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.phalanxdev.python;

/**
 * For session locking.
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 */
public class SessionMutex {
  private boolean verbose;

  /**
   * defines the current mutex state
   */
  private boolean locked;

  /**
   * thread that m_locked this mutex
   */
  private Thread lockedBy;

  public SessionMutex() {
  }

  public SessionMutex( boolean verbose ) {
    this.verbose = verbose;
  }

  private synchronized void lock() {
    while ( locked ) {
      if ( lockedBy == Thread.currentThread() ) {
        System.err.println( "INFO: Mutex detected a deadlock! The application is likely to hang indefinitely!" );
      }

      if ( verbose ) {
        System.out.println( "INFO: " + toString() + " is m_locked by " + lockedBy + ", but " + Thread.currentThread()
            + " waits for release" );
      }
      try {
        wait();
      } catch ( InterruptedException e ) {
        if ( verbose )
          System.out.println( "INFO: " + toString() + " caught InterruptedException" );
      }
    }
    locked = true;
    lockedBy = Thread.currentThread();
    if ( verbose ) {
      System.out.println( "INFO: " + toString() + " m_locked by " + lockedBy );
    }
  }

  public synchronized boolean safeLock() {
    if ( locked && lockedBy == Thread.currentThread() ) {
      if ( verbose ) {
        System.out.println( "INFO: " + toString() + " unable to provide safe lock for " + Thread.currentThread() );
      }
      return false;
    }
    lock();
    return true;
  }

  public synchronized void unlock() {
    if ( locked && lockedBy != Thread.currentThread() ) {
      System.err.println( "WARNING: Mutex was unlocked by other thread" );
    }
    locked = false;
    if ( verbose ) {
      System.out.println( "INFO: " + toString() + " unlocked by " + Thread.currentThread() );
    }

    // notify just 1 in case more of them are waiting
    notify();
  }
}
