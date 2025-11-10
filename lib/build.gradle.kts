import org.gradle.api.plugins.antlr.AntlrTask
import org.gradle.jvm.tasks.Jar
import org.gradle.api.tasks.testing.logging.TestLogEvent

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
    implementation(libs.calcite.core)
    implementation(libs.calcite.linq4j)
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

fun readDriverVersion(): String {
    val versionFile = layout.projectDirectory.file("src/main/java/com/beancount/jdbc/Version.java").asFile
    logger.info("Version file: ${versionFile.absolutePath}, exists=${versionFile.exists()}")
    if (!versionFile.exists()) {
        return "unknown"
    }
    val contents = versionFile.readText()
    val literalRegex = Regex("""public\s+static\s+final\s+String\s+FULL\s*=\s*"([^"]+)"""")
    val literalMatch = literalRegex.find(contents)?.groupValues?.get(1)
    if (literalMatch != null) {
        return literalMatch
    }
    val major = Regex("""MAJOR\s*=\s*(\d+)""").find(contents)?.groupValues?.get(1)?.toIntOrNull()
    val minor = Regex("""MINOR\s*=\s*(\d+)""").find(contents)?.groupValues?.get(1)?.toIntOrNull()
    val patch = Regex("""PATCH\s*=\s*(\d+)""").find(contents)?.groupValues?.get(1)?.toIntOrNull()
    val qualifier = Regex("""QUALIFIER\s*=\s*"([^"]+)"""").find(contents)?.groupValues?.get(1)
    if (major != null && minor != null && patch != null && qualifier != null) {
        return "$major.$minor.$patch-$qualifier"
    }
    return "unknown"
}

val driverVersion = readDriverVersion()
version = driverVersion

gradle.taskGraph.whenReady {
    logger.lifecycle("beancount-jdbc version: $driverVersion")
}


tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
    testLogging {
        events(TestLogEvent.FAILED)
        showStandardStreams = false
    }
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
    doFirst {
        println("Assembling beancount-jdbc version: $driverVersion")
    }
    manifest {
        attributes["Implementation-Version"] = driverVersion
    }
}

tasks.register<Copy>("copyRuntimeClasspath") {
    description = "Copies runtime dependencies into build/runtime-libs for external tools"
    group = "distribution"
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("runtime-libs"))
}
