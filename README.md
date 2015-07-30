# Function Passing: Typed, Distributed Functional Programming

[![Build Status](https://travis-ci.org/heathermiller/f-p.svg?branch=master)](https://travis-ci.org/heathermiller/f-p/)

The `F-P` library uses [sbt](http://www.scala-sbt.org/) for building and
testing. Distributed tests and examples are supported using the
[sbt-multi-jvm](https://github.com/sbt/sbt-multi-jvm) sbt plug-in.

Homepage: [http://lampwww.epfl.ch/~hmiller/f-p](http://lampwww.epfl.ch/~hmiller/f-p)

## How to Run Examples and Tests

Examples and tests can be launched from the sbt prompt:

```
> multi-jvm:run netty.Basic
> multi-jvm:run netty.WordCount
```
