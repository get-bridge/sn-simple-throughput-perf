package com.bridge.flink.perf.generator.formats

import com.bridge.flink.formats.debezium.Op
import com.bridge.flink.formats.debezium.Source
import com.bridge.flink.formats.learn.LearnPayload
import com.bridge.flink.formats.learn.LearnUser
import com.bridge.flink.formats.learn.LearnUserEnvelope
import java.util.*

class LearnUserGenerator {

    @Volatile
    private var idSource = 0
    fun generate(): LearnUser {
        val now = System.currentTimeMillis()
        return LearnUser(
            id = ++idSource,
            email = "some@email.com",
            uid = "1241415125125",
            createdAt = now,
            updatedAt = now,
            avatarUrl = null,
            firstName = "FirstName",
            lastName = "LastName",
            domainId = 1,
            deletedAt = null,
            tagline = "Some tagline",
            locale = "hu_HU",
            loggedInAt = now,
            fullName = "Full Name",
            sortableName = "Sortable Name",
            uuid = UUID.randomUUID().toString(),
            subAccountId = 1,
            hireDate = now,
            hidden = false,
            jobTitle = "Job Title",
            bio = "Bio",
            department = "Department",
        )
    }

    fun generateUserEnvelope() = LearnUserEnvelope(
        op = Op.CREATE,
        source = Source(
            version = "2.1.2.Final",
            connector = "postgresql",
            name = "tutorial",
            tsMs = System.currentTimeMillis(),
            snapshot = "false",
            db = "some_wonderful_db",
            sequence = "[\"49902520\",\"49902576\"]",
            schema = "bridge",
            table = "users",
            txId = 56612345,
            lsn = 125135,
            xmin = null
        ),
        payload = LearnPayload(
            after = generate()
        )
    )
}