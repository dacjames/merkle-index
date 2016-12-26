package io.dac.tsindex.util

import java.security.MessageDigest
import scala.util.hashing.MurmurHash3
/**
  * Created by dcollins on 12/26/16.
  */
trait StringHasher {

  private val md5instance =
    MessageDigest.getInstance("MD5")

  def md5(s: String): String =
    md5instance.digest(s.getBytes).map("%02x".format(_)).mkString

  def murmur(s: String): String =
    Integer.toHexString(MurmurHash3.stringHash(s))

  def incHash[A](a: String, b: A): String =
    this.md5(a ++ b.toString)

  def chainHash[A](nodes: Seq[A]): String =
    this.chainHash(nodes, "")

  def chainHash[A](nodes: Seq[A], hashCode: String): String =
    nodes.foldLeft(hashCode)(incHash)
}

