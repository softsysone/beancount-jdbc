package com.beancount.jdbc.loader.validation;

import com.beancount.jdbc.ledger.BalanceRecord;
import com.beancount.jdbc.ledger.CloseRecord;
import com.beancount.jdbc.ledger.DocumentRecord;
import com.beancount.jdbc.ledger.LedgerData;
import com.beancount.jdbc.ledger.LedgerEntry;
import com.beancount.jdbc.ledger.NoteRecord;
import com.beancount.jdbc.ledger.OpenRecord;
import com.beancount.jdbc.ledger.PadRecord;
import com.beancount.jdbc.ledger.PostingRecord;
import com.beancount.jdbc.loader.LoaderMessage;
import com.beancount.jdbc.loader.LoaderMessage.Level;
import com.beancount.jdbc.loader.semantic.SemanticAnalysis;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Mirrors Beancount's grammar-level account name validation by reusing the same component
 * constraints (root must begin with an uppercase letter; subsequent components must begin with an
 * uppercase letter or digit).
 */
final class AccountNameValidationRule implements ValidationRule {

    // Matches ACC_COMP_TYPE_RE and ACC_COMP_NAME_RE from beancount.core.account / parser.grammar.
    private static final String ACC_COMP_TYPE_RE = "\\p{Lu}[\\p{L}\\p{Nd}-]*";
    private static final String ACC_COMP_NAME_RE = "[\\p{Lu}\\p{Nd}][\\p{L}\\p{Nd}-]*";
    private static final Pattern ACCOUNT_PATTERN =
            Pattern.compile("(?:" + ACC_COMP_TYPE_RE + ")(?::" + ACC_COMP_NAME_RE + ")+");

    @Override
    public List<LoaderMessage> validate(SemanticAnalysis analysis) {
        LedgerData ledgerData = analysis.getLedgerData();
        Map<Integer, LedgerEntry> entriesById = indexEntries(ledgerData.getEntries());
        List<LoaderMessage> messages = new ArrayList<>();

        for (OpenRecord record : ledgerData.getOpens()) {
            validateAccount(record.getAccount(), entriesById.get(record.getEntryId()), messages);
        }
        for (CloseRecord record : ledgerData.getCloses()) {
            validateAccount(record.getAccount(), entriesById.get(record.getEntryId()), messages);
        }
        for (PadRecord record : ledgerData.getPads()) {
            LedgerEntry entry = entriesById.get(record.getEntryId());
            validateAccount(record.getAccount(), entry, messages);
            validateAccount(record.getSourceAccount(), entry, messages);
        }
        for (BalanceRecord record : ledgerData.getBalances()) {
            validateAccount(record.getAccount(), entriesById.get(record.getEntryId()), messages);
        }
        for (NoteRecord record : ledgerData.getNotes()) {
            validateAccount(record.getAccount(), entriesById.get(record.getEntryId()), messages);
        }
        for (DocumentRecord record : ledgerData.getDocuments()) {
            validateAccount(record.getAccount(), entriesById.get(record.getEntryId()), messages);
        }
        for (PostingRecord record : ledgerData.getPostings()) {
            validateAccount(record.getAccount(), entriesById.get(record.getEntryId()), messages);
        }

        return messages;
    }

    private static Map<Integer, LedgerEntry> indexEntries(List<LedgerEntry> entries) {
        Map<Integer, LedgerEntry> map = new HashMap<>(entries.size());
        for (LedgerEntry entry : entries) {
            map.put(entry.getId(), entry);
        }
        return map;
    }

    private static void validateAccount(String account, LedgerEntry entry, List<LoaderMessage> out) {
        if (account == null || account.isBlank()) {
            return;
        }
        if (ACCOUNT_PATTERN.matcher(account).matches()) {
            return;
        }
        String filename = entry != null ? entry.getSourceFilename() : "";
        int line = entry != null ? entry.getSourceLineno() : 0;
        out.add(new LoaderMessage(Level.ERROR, "Invalid account name: " + account, filename, line));
    }
}
