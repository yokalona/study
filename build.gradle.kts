import java.util.*

plugins {
    id("java")
    id("me.champeau.jmh") version "0.7.2"
}

group = "com.yokalona"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    jmh("org.openjdk.jmh:jmh-core:1.36")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:1.36")
    jmhAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.36")
}

tasks.test {
    useJUnitPlatform()
}

jmh {
    timeOnIteration = "1s"
    iterations = 1
    warmupIterations = 1
    warmup = "1s"
    warmupForks = 1

    fork = 2
    profilers.add("com.yokalona.tree.b.ComparisonCountProfiler")
    resultFormat = "JSON"
    resultsFile = project.file("${project.layout.buildDirectory.get().asFile.name}/benchmarks/output.json")
}
