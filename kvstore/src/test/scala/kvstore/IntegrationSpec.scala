/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import org.scalatest.{FunSuiteLike, Matchers}

trait IntegrationSpec
  extends FunSuiteLike
        with Matchers { this: KVStoreSuite =>

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */
  }
