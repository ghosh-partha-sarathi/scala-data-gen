FROM gitpod/workspace-full:2022-05-19-23-37-25

# installing scala into image via HomeBrew
RUN brew install scala

# installing Ammonite REPL
RUN sudo sh -c '(echo "#!/usr/bin/env sh" && curl -L https://github.com/lihaoyi/Ammonite/releases/download/2.5.3/3.1-2.5.3) > /usr/local/bin/amm && chmod +x /usr/local/bin/amm'

# installing version manager for scala
RUN brew install scalaenv

# sample statement installing scala version 3.0.0
RUN scalaenv install scala3-3.0.0 && scalaenv global scala3-3.0.0

# installing sbt for build
RUN brew install sbt

# Coursier is an artifact fetcher for Scala
RUN brew install coursier/formulas/coursier

# installing scalafmt for formatting Scala code
RUN sudo env "PATH=$PATH" coursier bootstrap org.scalameta:scalafmt-cli_2.12:2.4.2 \
  -r sonatype:snapshots \
  -o /usr/local/bin/scalafmt --standalone --main org.scalafmt.cli.Cli  