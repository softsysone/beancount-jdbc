plugins {
    base
}

allprojects {
    repositories {
        mavenCentral()
    }
}

tasks.withType<org.gradle.api.tasks.testing.Test>().configureEach {
    val versionLabel = project.version.toString()
    doLast {
        logger.lifecycle("Completed ${path} for version $versionLabel")
    }
}

subprojects {
    tasks.matching { it.name == "jar" }.configureEach {
        val versionLabel = project.version.toString()
        doLast {
            logger.lifecycle("Completed ${path} for version $versionLabel")
        }
    }
}
