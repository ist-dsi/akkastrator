# Akkastrator [![license](http://img.shields.io/:license-MIT-blue.svg)](LICENSE)
[![Scaladoc](http://javadoc-badge.appspot.com/pt.tecnico.dsi/akkastrator_2.12.svg?label=scaladoc&style=plastic&maxAge=604800)](https://ist-dsi.github.io/akkastrator/latest/api/pt/tecnico/dsi/akkastrator/index.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/pt.tecnico.dsi/akkastrator_2.12/badge.svg?maxAge=604800)](https://maven-badges.herokuapp.com/maven-central/pt.tecnico.dsi/akkastrator_2.12)
[![Dependency Status](https://www.versioneye.com/java/pt.tecnico.dsi:akkastrator_2.12/badge.svg?style=plastic&maxAge=604800)](https://www.versioneye.com/java/pt.tecnico.dsi:akkastrator_2.12/)
[![Reference Status](https://www.versioneye.com/java/pt.tecnico.dsi:akkastrator_2.12/reference_badge.svg?style=plastic&maxAge=604800)](https://www.versioneye.com/java/pt.tecnico.dsi:akkastrator_2.12/references)


[![Build Status](https://travis-ci.org/ist-dsi/akkastrator.svg?branch=master&style=plastic&maxAge=604800)](https://travis-ci.org/ist-dsi/akkastrator)
[![Codacy Badge](https://api.codacy.com/project/badge/coverage/75210854e9b945df97a8408e4975a067)](https://www.codacy.com/app/IST-DSI/akkastrator)
[![Codacy Badge](https://api.codacy.com/project/badge/grade/75210854e9b945df97a8408e4975a067)](https://www.codacy.com/app/IST-DSI/akkastrator)
[![BCH compliance](https://bettercodehub.com/edge/badge/ist-dsi/akkastrator)](https://bettercodehub.com/)

[Latest scaladoc documentation](https://ist-dsi.github.io/akkastrator/latest/api/pt/tecnico/dsi/akkastrator/index.html)

## Install
Add the following dependency to your `build.sbt`:
```sbt
libraryDependencies += "pt.tecnico.dsi" %% "akkastrator" % "0.8.0"
```
We use [semantic versioning](http://semver.org).


## Notes
The content of the sent messages are not persisted to the journal, however the content of the received responses is
persisted to the journal. This has the following implications:

1. It is not possible to reap the benefits of Event Sourcing since the requests are not journaled. However it is possible
to implement ES on top of akkastrator by sending aditional messages inside the orchestrators.
2. If sensitive data is sent in the responses be sure to secure your journals as the data will be journaled there. 

## License
Akkastrator is open source and available under the [MIT license](LICENSE).
