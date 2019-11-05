## Usage: 
+ Compile from source code:  

```bash   
mvn clean compile
```

+ Generate jar without dependencies:   

```bash
mvn jar  // or   
mvn clean compile jar
```

+ Generate jar with all dependencies:  

```bash
mvn assembly:single  // or
mvn clean compile assembly:single
```

The compiled code and jar file will be in located in _target_ folder. 





