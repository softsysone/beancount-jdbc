package com.beancount.jdbc.loader.semantic.inventory;

import com.beancount.jdbc.ledger.PostingRecord;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.math.RoundingMode;

/**
 * Minimal inventory book that mirrors Beancount's lot tracking.
 *
 * <p>Each account maintains per-commodity inventories. Positive lots are stored FIFO so we can
 * release them as shares are sold, while short lots are tracked separately until they are closed.
 */
public final class InventoryTracker {

    private final Map<String, AccountInventory> inventoryByAccount = new HashMap<>();
    private BookingMethod bookingMethod = BookingMethod.FIFO;

    public void setBookingMethod(BookingMethod method) {
        if (method != null) {
            this.bookingMethod = method;
        }
    }

    public static final class StrictBookingException extends RuntimeException {
        public StrictBookingException(String message) {
            super(message);
        }
    }

    public void applyPostings(List<PostingRecord> postings) {
        if (postings == null) {
            return;
        }
        for (PostingRecord posting : postings) {
            applyPosting(posting);
        }
    }

    public boolean hasCostedHoldings(String account, String currency) {
        if (account == null || currency == null) {
            return false;
        }
        AccountInventory accountInventory = inventoryByAccount.get(account);
        if (accountInventory == null) {
            return false;
        }
        return accountInventory.hasCostedHoldings(currency);
    }

    // Visible for tests.
    List<InventoryLot> lots(String account, String currency) {
        AccountInventory accountInventory = inventoryByAccount.get(account);
        if (accountInventory == null) {
            return List.of();
        }
        return accountInventory.lots(currency);
    }

    private void applyPosting(PostingRecord posting) {
        BigDecimal units = posting.getNumber();
        if (units == null) {
            return;
        }
        String account = posting.getAccount();
        String currency = posting.getCurrency();
        if (account == null || currency == null || units.compareTo(BigDecimal.ZERO) == 0) {
            return;
        }
        inventoryByAccount
                .computeIfAbsent(account, key -> new AccountInventory())
                .apply(currency, units, posting, bookingMethod);
    }

    private static final class AccountInventory {
        private final Map<String, CommodityInventory> commodities = new HashMap<>();

        void apply(String currency, BigDecimal units, PostingRecord posting, BookingMethod method) {
            commodities
                    .computeIfAbsent(currency, key -> new CommodityInventory())
                    .apply(units, posting, method);
        }

        boolean hasCostedHoldings(String currency) {
            CommodityInventory commodity = commodities.get(currency);
            return commodity != null && commodity.hasCostedHoldings();
        }

        List<InventoryLot> lots(String currency) {
            CommodityInventory commodity = commodities.get(currency);
            if (commodity == null) {
                return List.of();
            }
            return commodity.snapshot();
        }
    }

    private static final class CommodityInventory {
        private final Deque<InventoryLot> positiveLots = new ArrayDeque<>();
        private final Deque<InventoryLot> shortLots = new ArrayDeque<>();

        void apply(BigDecimal units, PostingRecord posting, BookingMethod method) {
            BookingMethod mode = method != null ? method : BookingMethod.FIFO;
            if (units.signum() > 0) {
                addPositive(units, posting, mode);
            } else if (units.signum() < 0) {
                addNegative(units.abs(), posting, mode);
            }
        }

        boolean hasCostedHoldings() {
            for (InventoryLot lot : positiveLots) {
                if (lot.isCosted()) {
                    return true;
                }
            }
            return false;
        }

        List<InventoryLot> snapshot() {
            List<InventoryLot> result = new ArrayList<>(positiveLots.size() + shortLots.size());
            for (InventoryLot lot : positiveLots) {
                result.add(lot.copy());
            }
            for (InventoryLot lot : shortLots) {
                result.add(lot.copy());
            }
            return result;
        }

        private void addPositive(BigDecimal units, PostingRecord posting, BookingMethod method) {
            BigDecimal remaining = units;
            while (!shortLots.isEmpty() && remaining.compareTo(BigDecimal.ZERO) > 0) {
                InventoryLot shortLot = shortLots.peekFirst();
                BigDecimal shortAbs = shortLot.getUnits().abs();
                int cmp = shortAbs.compareTo(remaining);
                if (cmp > 0) {
                    shortLot.adjustUnits(shortLot.getUnits().add(remaining));
                    remaining = BigDecimal.ZERO;
                } else {
                    remaining = remaining.subtract(shortAbs);
                    shortLots.removeFirst();
                }
            }
            if (remaining.compareTo(BigDecimal.ZERO) > 0) {
                if (method == BookingMethod.STRICT) {
                    ensureStrictCost(posting);
                    positiveLots.addLast(InventoryLot.fromPosting(remaining, posting));
                } else if (method == BookingMethod.AVERAGE) {
                    addAverageLot(remaining, posting);
                } else {
                    InventoryLot lot = InventoryLot.fromPosting(remaining, posting);
                    positiveLots.addLast(lot);
                }
            }
        }

        private void addAverageLot(BigDecimal units, PostingRecord posting) {
            if (positiveLots.isEmpty()) {
                positiveLots.addLast(InventoryLot.fromPosting(units, posting));
                return;
            }
            InventoryLot lot = positiveLots.peekFirst();
            BigDecimal existingUnits = lot.getUnits();
            BigDecimal totalUnits = existingUnits.add(units);

            BigDecimal existingCost = lot.getCostNumber();
            BigDecimal newCost = posting.getCostNumber();
            String costCurrency = lot.getCostCurrency() != null
                    ? lot.getCostCurrency()
                    : posting.getCostCurrency();
            BigDecimal averageCost = existingCost;
            if (existingCost != null && newCost != null) {
                BigDecimal existingValue = existingCost.multiply(existingUnits);
                BigDecimal newValue = newCost.multiply(units);
                int scale = Math.max(Math.max(existingCost.scale(), newCost.scale()), 8);
                averageCost =
                        existingValue.add(newValue).divide(totalUnits, scale, RoundingMode.HALF_EVEN);
            } else if (existingCost == null && newCost != null) {
                averageCost = newCost;
            }

            positiveLots.clear();
            positiveLots.addLast(InventoryLot.averageLot(totalUnits, averageCost, costCurrency));
        }

        private void addNegative(BigDecimal quantity, PostingRecord posting, BookingMethod method) {
            BigDecimal remaining = quantity;
            if (method == BookingMethod.STRICT) {
                ensureStrictCost(posting);
                InventoryLot match = findStrictMatch(posting);
                if (match == null) {
                    throw new StrictBookingException(
                            "booking_method STRICT could not find lot for account " + posting.getAccount());
                }
                if (match.getUnits().compareTo(remaining) < 0) {
                    throw new StrictBookingException(
                            "booking_method STRICT has insufficient units for account " + posting.getAccount());
                }
                match.adjustUnits(match.getUnits().subtract(remaining));
                if (match.getUnits().compareTo(BigDecimal.ZERO) == 0) {
                    positiveLots.remove(match);
                }
                return;
            }
            while (!positiveLots.isEmpty() && remaining.compareTo(BigDecimal.ZERO) > 0) {
                InventoryLot lot =
                        method == BookingMethod.LIFO ? positiveLots.peekLast() : positiveLots.peekFirst();
                BigDecimal lotUnits = lot.getUnits();
                int cmp = lotUnits.compareTo(remaining);
                if (cmp > 0) {
                    lot.adjustUnits(lotUnits.subtract(remaining));
                    remaining = BigDecimal.ZERO;
                } else {
                    remaining = remaining.subtract(lotUnits);
                    if (method == BookingMethod.LIFO) {
                        positiveLots.removeLast();
                    } else {
                        positiveLots.removeFirst();
                    }
                }
            }
            if (remaining.compareTo(BigDecimal.ZERO) > 0) {
                shortLots.addLast(InventoryLot.shortLot(remaining.negate()));
            }
        }

        private void ensureStrictCost(PostingRecord posting) {
            if (posting.getCostCurrency() == null || posting.getCostNumber() == null) {
                throw new StrictBookingException(
                        "booking_method STRICT requires explicit cost on posting for account "
                                + posting.getAccount());
            }
        }

        private InventoryLot findStrictMatch(PostingRecord posting) {
            InventoryLot match = null;
            for (InventoryLot lot : positiveLots) {
                if (lot.matchesPosting(posting)) {
                    if (match != null && match != lot) {
                        throw new StrictBookingException(
                                "booking_method STRICT found ambiguous lots for account "
                                        + posting.getAccount());
                    }
                    match = lot;
                }
            }
            return match;
        }
    }

    public static final class InventoryLot {
        private BigDecimal units;
        private final BigDecimal costNumber;
        private final String costCurrency;
        private final LocalDate costDate;
        private final String costLabel;

        private InventoryLot(
                BigDecimal units,
                BigDecimal costNumber,
                String costCurrency,
                LocalDate costDate,
                String costLabel) {
            this.units = Objects.requireNonNull(units, "units");
            this.costNumber = costNumber;
            this.costCurrency = costCurrency;
            this.costDate = costDate;
            this.costLabel = costLabel;
        }

        static InventoryLot fromPosting(BigDecimal units, PostingRecord posting) {
            return new InventoryLot(
                    units,
                    posting.getCostNumber(),
                    posting.getCostCurrency(),
                    posting.getCostDate(),
                    posting.getCostLabel());
        }

        static InventoryLot shortLot(BigDecimal units) {
            return new InventoryLot(units, null, null, null, null);
        }

        BigDecimal getUnits() {
            return units;
        }

        void adjustUnits(BigDecimal updated) {
            this.units = updated;
        }

        BigDecimal getCostNumber() {
            return costNumber;
        }

        String getCostCurrency() {
            return costCurrency;
        }

        boolean isCosted() {
            return costNumber != null || costCurrency != null || costDate != null || costLabel != null;
        }

        public InventoryLot copy() {
            return new InventoryLot(units, costNumber, costCurrency, costDate, costLabel);
        }

        static InventoryLot averageLot(BigDecimal units, BigDecimal perUnitCost, String costCurrency) {
            return new InventoryLot(units, perUnitCost, costCurrency, null, null);
        }

        boolean matchesPosting(PostingRecord posting) {
            return matchesDecimal(posting.getCostNumber(), costNumber)
                    && matchesString(posting.getCostCurrency(), costCurrency)
                    && matchesDate(posting.getCostDate(), costDate)
                    && matchesString(posting.getCostLabel(), costLabel);
        }

        private static boolean matchesDecimal(BigDecimal postingValue, BigDecimal lotValue) {
            if (postingValue == null) {
                return true;
            }
            if (lotValue == null) {
                return false;
            }
            return postingValue.compareTo(lotValue) == 0;
        }

        private static boolean matchesString(String postingValue, String lotValue) {
            return postingValue == null || Objects.equals(postingValue, lotValue);
        }

        private static boolean matchesDate(LocalDate postingValue, LocalDate lotValue) {
            return postingValue == null || Objects.equals(postingValue, lotValue);
        }
    }
}
