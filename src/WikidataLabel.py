"""Label cache and lazy label resolution for Wikibase entities."""

import json
import os
from datetime import datetime, timedelta

from sqlalchemy import Column, DateTime, String, create_engine, text
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import declarative_base, sessionmaker

from .utils import get_wikidata_json_by_ids

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "label")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))

LABEL_UNLIMITED = os.environ.get("LABEL_UNLIMITED", "false") == "true"
LABEL_TTL_DAYS = int(os.environ.get("LABEL_TTL_DAYS", "90"))
LABEL_MAX_ROWS = int(os.environ.get("LABEL_MAX_ROWS", "10000000"))
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "15"))
DEFAULT_WIKIBASE_URL = "https://www.wikidata.org"

DATABASE_URL = f"mariadb+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

engine = create_engine(
    DATABASE_URL,
    pool_size=5,  # Limit the number of open connections
    max_overflow=10,  # Allow extra connections beyond pool_size
    pool_recycle=1800,  # Recycle connections every 30 minutes
    pool_pre_ping=True,
)

Base = declarative_base()
Session = sessionmaker(bind=engine, expire_on_commit=False)


class WikidataLabel(Base):
    """Database cache for multilingual Wikibase labels."""

    __tablename__ = "labels"
    id = Column(String(64), primary_key=True)
    wikibase_url = Column(String(255), primary_key=True, default=DEFAULT_WIKIBASE_URL)
    labels = Column(JSON, default=dict)
    date_added = Column(DateTime, default=datetime.now, index=True)

    @staticmethod
    def initialize_database():
        """Create tables if they do not already exist."""
        try:
            Base.metadata.create_all(engine)
            WikidataLabel._migrate_labels_table_for_wikibase()
            return True
        except Exception as e:
            print(f"Error while initializing labels database: {e}")
            return False

    @staticmethod
    def _migrate_labels_table_for_wikibase():
        """Ensure the labels table supports cache partitioning per Wikibase."""
        with engine.begin() as connection:
            has_wikibase_url = connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = :schema_name
                      AND TABLE_NAME = 'labels'
                      AND COLUMN_NAME = 'wikibase_url'
                    """
                ),
                {"schema_name": DB_NAME},
            ).scalar()

            if not has_wikibase_url:
                connection.execute(
                    text(
                        f"""
                        ALTER TABLE labels
                        ADD COLUMN wikibase_url VARCHAR(255) NOT NULL DEFAULT '{DEFAULT_WIKIBASE_URL}'
                        """
                    )
                )

            primary_key_cols = [
                row[0]
                for row in connection.execute(
                    text(
                        """
                        SELECT COLUMN_NAME
                        FROM information_schema.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = :schema_name
                          AND TABLE_NAME = 'labels'
                          AND CONSTRAINT_NAME = 'PRIMARY'
                        ORDER BY ORDINAL_POSITION
                        """
                    ),
                    {"schema_name": DB_NAME},
                ).fetchall()
            ]

            if primary_key_cols == ["id"]:
                connection.execute(
                    text(
                        """
                        ALTER TABLE labels
                        DROP PRIMARY KEY,
                        ADD PRIMARY KEY (id, wikibase_url)
                        """
                    )
                )

    @staticmethod
    def add_bulk_labels(data, wb_url: str = DEFAULT_WIKIBASE_URL):
        """Insert or update multiple label records.

        Args:
            data (list[dict]): Records containing at least ``id`` and ``labels`` keys.
            wb_url (str): Wikibase URL used as cache partition key.

        Returns:
            bool: ``True`` when the operation succeeds, otherwise ``False``.
        """
        if not data:
            return True

        normalized_wb_url = WikidataLabel._normalize_wb_url(wb_url)
        rows = []
        for row in data:
            normalized_row = {
                "id": row["id"],
                "wikibase_url": WikidataLabel._normalize_wb_url(row.get("wikibase_url", normalized_wb_url)),
                "labels": row.get("labels", {}),
                "date_added": datetime.now(),
            }
            if isinstance(normalized_row["labels"], dict):
                normalized_row["labels"] = json.dumps(
                    normalized_row["labels"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            rows.append(normalized_row)

        with Session() as session:
            try:
                session.execute(
                    text("""
                    INSERT INTO labels (id, wikibase_url, labels, date_added)
                    VALUES (:id, :wikibase_url, :labels, :date_added)
                    ON DUPLICATE KEY UPDATE
                    labels = VALUES(labels),
                    date_added = VALUES(date_added)
                """),
                    rows,
                )

                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
                return False

    @staticmethod
    def add_label(id, labels, wb_url: str = DEFAULT_WIKIBASE_URL):
        """Insert or update labels for a single entity.

        Args:
            id (str): Entity ID.
            labels (dict): Mapping of language code to label text.
            wb_url (str): Wikibase URL used as cache partition key.

        Returns:
            bool: ``True`` when the operation succeeds, otherwise ``False``.
        """
        with Session() as session:
            try:
                new_entry = WikidataLabel(
                    id=id,
                    wikibase_url=WikidataLabel._normalize_wb_url(wb_url),
                    labels=labels,
                )
                session.add(new_entry)
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
                return False

    @staticmethod
    def get_labels(id, wb_url: str = DEFAULT_WIKIBASE_URL):
        """Retrieve cached labels for one entity, with API fallback.

        Args:
            id (str): Entity ID.
            wb_url (str): Wikibase URL used as cache partition key.

        Returns:
            dict | None: Cached or fetched labels for the entity, if available.
        """
        normalized_wb_url = WikidataLabel._normalize_wb_url(wb_url)
        try:
            with Session() as session:
                # Get labels that are less than LABEL_TTL_DAYS old
                date_limit = datetime.now() - timedelta(days=LABEL_TTL_DAYS)
                item = (
                    session.query(WikidataLabel)
                    .filter(
                        WikidataLabel.id == id,
                        WikidataLabel.wikibase_url == normalized_wb_url,
                        WikidataLabel.date_added >= date_limit,
                    )
                    .first()
                )

                if item is not None:
                    return item.labels or {}
        except Exception as e:
            print(f"Error while fetching cached label {id}: {e}")

        labels = WikidataLabel._get_labels_wdapi(id, wb_url=normalized_wb_url).get(id)
        if labels:
            WikidataLabel.add_label(id, labels, wb_url=normalized_wb_url)

        return labels

    @staticmethod
    def get_bulk_labels(ids, wb_url: str = DEFAULT_WIKIBASE_URL):
        """Retrieve cached labels for multiple entities, with API fallback.

        Args:
            ids (list[str]): Entity IDs to fetch.
            wb_url (str): Wikibase URL used as cache partition key.

        Returns:
            dict[str, dict]: Mapping of each requested ID to its labels.
        """
        if not ids:
            return {}

        normalized_wb_url = WikidataLabel._normalize_wb_url(wb_url)
        labels = {}
        try:
            with Session() as session:
                # Get labels that are less than LABEL_TTL_DAYS old
                date_limit = datetime.now() - timedelta(days=LABEL_TTL_DAYS)
                rows = (
                    session.query(WikidataLabel.id, WikidataLabel.labels)
                    .filter(
                        WikidataLabel.id.in_(ids),
                        WikidataLabel.wikibase_url == normalized_wb_url,
                        WikidataLabel.date_added >= date_limit,
                    )
                    .all()
                )
                labels = {id: labels for id, labels in rows}
        except Exception as e:
            print(f"Error while fetching cached labels in bulk: {e}")

        # Fallback when labels are missing from the database
        missing_ids = set(ids) - set(labels.keys())
        if missing_ids:
            missing_labels = WikidataLabel._get_labels_wdapi(missing_ids, wb_url=normalized_wb_url)
            labels.update(missing_labels)

            # Cache labels
            WikidataLabel.add_bulk_labels(
                [{"id": entity_id, "labels": entity_labels} for entity_id, entity_labels in missing_labels.items()],
                wb_url=normalized_wb_url,
            )

        return labels

    @staticmethod
    def delete_old_labels():
        """Delete expired labels and enforce maximum cache size.

        Returns:
            bool: ``True`` when cleanup succeeds or is skipped, otherwise ``False``.
        """
        if LABEL_UNLIMITED:
            return True

        with Session() as session:
            try:
                # Step 1: Delete labels older than X days
                date_limit = datetime.now() - timedelta(days=LABEL_TTL_DAYS)
                session.execute(text("DELETE FROM labels WHERE date_added < :date_limit"), {"date_limit": date_limit})
                session.commit()

                # Step 2: Check total count
                total_count = session.execute(text("SELECT COUNT(*) FROM labels")).scalar()

                if total_count > LABEL_MAX_ROWS:
                    # Calculate how many rows to delete
                    rows_to_delete = total_count - LABEL_MAX_ROWS

                    # Delete oldest rows by date_added (MySQL-safe form)
                    session.execute(
                        text("""
                            DELETE l
                            FROM labels AS l
                            JOIN (
                                SELECT id, wikibase_url
                                FROM labels
                                ORDER BY date_added ASC
                                LIMIT :rows_to_delete
                            ) AS old_labels
                              ON l.id = old_labels.id
                             AND l.wikibase_url = old_labels.wikibase_url
                        """),
                        {"rows_to_delete": rows_to_delete},
                    )

                    session.commit()

                return True
            except Exception as e:
                session.rollback()
                print(f"Error while deleting old labels: {e}")
                return False

    @staticmethod
    def _get_labels_wdapi(ids, wb_url: str = DEFAULT_WIKIBASE_URL):
        """Retrieve labels from the Wikibase Action API.

        Args:
            ids (list[str] | str): IDs as a list or ``|``-separated string.
            wb_url (str): Wikibase URL to query.

        Returns:
            dict[str, dict]: Mapping of each ID to compressed labels.
        """
        entities_data = get_wikidata_json_by_ids(ids, wb_url=wb_url, props="labels")
        entities_data = WikidataLabel._compress_labels(entities_data)
        return entities_data

    @staticmethod
    def _compress_labels(data):
        """Compress API labels by extracting each language's ``value`` field.

        Args:
            data (dict): Raw entities payload from the Wikidata API.

        Returns:
            dict[str, dict]: Mapping of entity ID to ``{lang: label}``.
        """
        new_labels = {}
        for qid, labels in data.items():
            if "labels" in labels:
                new_labels[qid] = {lang: label.get("value") for lang, label in labels["labels"].items()}
            else:
                new_labels[qid] = {}
        return new_labels

    @staticmethod
    def _normalize_wb_url(wb_url: str) -> str:
        """Normalize a Wikibase URL for stable cache keys."""
        normalized = (wb_url or DEFAULT_WIKIBASE_URL).strip().rstrip("/")
        return normalized or DEFAULT_WIKIBASE_URL

    @staticmethod
    def get_lang_val(data, lang="en", fallback_lang=None):
        """Return the best label text from a labels dictionary.

        Args:
            data (dict): Label dictionary keyed by language.
            lang (str): Preferred language code.
            fallback_lang (str | None): Optional fallback language code.

        Returns:
            str: Selected label text, or an empty string when missing.
        """
        label = data.get(lang, data.get("mul", {}))
        if fallback_lang and not label:
            label = data.get(fallback_lang, {})

        if isinstance(label, str):
            return label
        return label.get("value", "")

    @staticmethod
    def get_all_missing_labels_ids(data):
        """Collect all referenced IDs that may require label lookup.

        Args:
            data (dict | list): Nested entity structure to scan.

        Returns:
            set[str]: Referenced IDs that may be missing resolved labels.
        """
        ids_list = set()

        if isinstance(data, dict):
            if "property" in data:
                ids_list.add(data["property"])
            if ("unit" in data) and (data["unit"] != "1"):
                ids_list.add(data["unit"].split("/")[-1])
            if (
                ("datatype" in data)
                and ("datavalue" in data)
                and (data["datatype"] in ["wikibase-item", "wikibase-property"])
            ):
                ids_list.add(data["datavalue"]["value"]["id"])
            if ("claims" in data) and isinstance(data["claims"], dict):
                ids_list = ids_list | data["claims"].keys()

            for _, value in data.items():
                ids_list = ids_list | WikidataLabel.get_all_missing_labels_ids(value)

        elif isinstance(data, list):
            for item in data:
                ids_list = ids_list | WikidataLabel.get_all_missing_labels_ids(item)

        return ids_list


class LazyLabel:
    """Deferred label string that resolves via a shared factory."""

    def __init__(self, qid, factory):
        """Store the target entity ID and the lookup factory.

        Args:
            qid (str): Entity ID whose label should be resolved lazily.
            factory (LazyLabelFactory): Factory that performs batched label resolution.
        """
        self.qid = qid
        self.factory = factory

    def __str__(self):
        """Resolve and return the label text for the configured entity."""
        self.factory.resolve_all()
        return self.factory.get_label(self.qid)


class LazyLabelFactory:
    """Create and batch-resolve lazy Wikibase labels."""

    def __init__(self, lang="en", fallback_lang="en", wb_url: str = DEFAULT_WIKIBASE_URL):
        """Initialize a lazy label factory.

        Args:
            lang (str): Preferred language code.
            fallback_lang (str): Fallback language code.
            wb_url (str): Wikibase URL used for label lookups.
        """
        self.lang = lang
        self.fallback_lang = fallback_lang
        self.wb_url = WikidataLabel._normalize_wb_url(wb_url)
        self._pending_ids = set()
        self._resolved_labels = {}

    def create(self, qid: str) -> "LazyLabel":
        """Create a lazy label handle and queue its ID for resolution.

        Args:
            qid (str): Entity ID to resolve.

        Returns:
            LazyLabel: Lazy label wrapper bound to this factory.
        """
        self._pending_ids.add(qid)
        return LazyLabel(qid, factory=self)

    def resolve_all(self):
        """Resolve all pending IDs in a single bulk lookup."""
        if not self._pending_ids:
            return

        self._pending_ids = self._pending_ids - set(self._resolved_labels.keys())
        label_data = WikidataLabel.get_bulk_labels(list(self._pending_ids), wb_url=self.wb_url)
        self._resolved_labels.update(label_data)
        self._pending_ids.clear()

    def get_label(self, qid: str) -> str:
        """Return the resolved label text for an entity ID.

        Args:
            qid (str): Entity ID.

        Returns:
            str: Best label text according to current language settings.
        """
        label_dict = self._resolved_labels.get(qid, {})
        label = WikidataLabel.get_lang_val(label_dict, lang=self.lang, fallback_lang=self.fallback_lang)
        return label

    def set_lang(self, lang: str):
        """Update preferred language and resolve pending IDs.

        Args:
            lang (str): Preferred language code.
        """
        self.lang = lang
        self.resolve_all()
