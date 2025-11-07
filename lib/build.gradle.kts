import org.gradle.api.plugins.antlr.AntlrTask
import org.gradle.jvm.tasks.Jar

plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
    antlr
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    antlr(libs.antlr4)
    implementation(libs.antlr4.runtime)
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}

tasks.withType<AntlrTask>().configureEach {
    arguments.addAll(listOf("-visitor", "-long-messages"))
    outputDirectory = file("$buildDir/generated-src/antlr/main")
}

sourceSets {
    named("main") {
        java.srcDir("$buildDir/generated-src/antlr/main")
    }
}

tasks.named<Jar>("jar") {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(
        configurations.runtimeClasspath.get()
            .filter { it.name.contains("antlr4-runtime") }
            .map { zipTree(it) }
    )
}
