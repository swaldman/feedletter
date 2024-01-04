# feedletter

## More to come later...

### Developer Notes

#### Lifecycle

1. A `feed` is added
2. One or more `subscribable`s is defined against the feed
3. One or more `destination`s subscribe to the feed.
4. `item`s are observed in the feed, and are added in the `Unassigned` state
5. Each `item` is assigned, in a single transaction, to all the collections (`assignable`s) to which they will ever belong.

(Steps 4 and 5 can repeat arbitrarily as new items come in.)

6. Separately, collections (`assignable`s) are periodically marked "complete"
   and, in the same transaction forwarded to subscribers.
7. Complete `assignable`s can be deleted, along with their `assignment`s
   Currently, for each subscription definition (`subscribable`) the one latest
   complete assignment is retained. That seems unnecessarily complex, and may
   change.
9. `item`s that are...
   * Already assigned
   * No longer belong to not-yet-completed `assignables` can drop their cached contents,
     and then move into the `Cleared` state.

#### Database Schema

I want to sketch the not-so-obvious db schema I've adopted for this project while
I still understand it.

##### feed

First there are feeds:

```sql
CREATE TABLE feed(
  id                          INTEGER,
  url                         VARCHAR(1024),
  min_delay_minutes           INTEGER NOT NULL,
  await_stabilization_minutes INTEGER NOT NULL,
  max_delay_minutes           INTEGER NOT NULL,
  added                       TIMESTAMP NOT NULL,
  last_assigned               TIMESTAMP NOT NULL,     -- we'll start at added
  PRIMARY KEY(id)
)
```

Feeds must be defined before subscriptions can be created against them.
They are defined by a URL, and they define what it means for a feed to
"finalize", in the sense of being ready for notification.

Feeds are permanent and basically unchanging until (when someday I implement
this) they are manually removed. 

(`last_assigned` changes, but so far it's
just informational, has no role in the application.)

##### item

Next there are items:

```sql
CREATE TABLE item(
  feed_id          INTEGER,
  guid             VARCHAR(1024),
  single_item_rss  TEXT,
  content_hash     INTEGER, -- ItemContent.contentHash
  link             VARCHAR(1024),
  first_seen       TIMESTAMP NOT NULL,
  last_checked     TIMESTAMP NOT NULL,
  stable_since     TIMESTAMP NOT NULL,
  assignability    ItemAssignability NOT NULL,
  PRIMARY KEY(feed_id, guid),
  FOREIGN KEY(feed_id) REFERENCES feed(id)
)""".stripMargin
```

* `feed_id` and `guid` identify an item.
* `single_item_rss` _caches_ the RSS item.
   We want to cache this, in case by the time we get around to notifying, the item is no longer available in the feed.
* `content_hash` is a hash based on the prior five fields. We use it to identify whether an item has changed.
* `link` may eventually be used as a neurotic double-check so we never notify the same human-perceived item twice
* `first_seen`, `last_checked`, and `stable_since` are pretty self-explanatory timestamps, We use these to
  calculate whether an item has stabilized and so can be "assigned". (See below.)
* `assignability`: items can be in one of four states
    * `Unassigned` — The item has not yet been assigned to the collections (including single member collections)
      to which it will eventually belong, but is eligible for assignment.
    * `Assigned` — The item _hash_ been assigned to _all_ the collections (including single member collections)
      to which it will eventually belong. The application may not be done assigning to those collections, and the
      items may not yet be distributed to subscribers.
    * `Cleared` — This is the terminal state for an item. The item has been assigned to all collections, and have
      already been distributed to subscribers. The cache fields (`title`, `author`, `article`, `publication_date`, and `link`)
      should all be cleared in this state. `Cleared` items are not deleted, but retained indefinitely, so that we don't
      renotify if the item (an item with the same `guid`) reappears in the feed.
    * `Excluded` — Items which are marked to always be ignored.

##### subscribable (subscription definition)

Next there is `subscribable`, which represents the definition of a subscription by which parties will be
notified of items or collections of items.

```sql
CREATE TABLE subscribable(
  subscribable_name         VARCHAR(64),
  feed_id                   INTEGER NOT NULL,
  subscription_manager_json JSONB NOT NULL,
  PRIMARY KEY (subscribable_name),
  FOREIGN KEY (feed_id) REFERENCES feed(id)
)
```

A subscribable maps a name to a feed and a `SubscriptionManager`. For our purposes here,
the main role of a `SubscriptionManager` (a serialization of a Scala ADT) is to

1. Generate for items a `within_type_id`, which is really just a collection identifier.
   All items in a collection of items that will be distributed will share the same `within_type_id`.
2. Determine whether a collection (identified by its `within_type_id`) is "complete" — that is,
   no further items need by assigned the same `within_type_id`.

`SubscriptionManager` determines how collections are compiled, to what kind of destination (e-mail,
Mastodon, mobile message, whatever) notifications will be sent, and how they will be formatted.

Names are scoped on a per-feed-URL basis. Users subscribe to a `(feed_url, subscribable_name)`
pair.

##### assignable (a collection of items)

Next there is `assignable`, which represents a collection. They essentially map
`subscribables` (subscription definitions) to `within_type_id`s (the collections
generated by the subscription definition and notified to subscribers).

```sql
CREATE TABLE assignable(
  subscribable_name VARCHAR(64),
  within_type_id    VARCHAR(1024),
  opened            TIMESTAMP NOT NULL,
  completed         TIMESTAMP,
  PRIMARY KEY(subscribable_name, within_type_id),
  FOREIGN KEY(subscribable_name) REFERENCES subscribable(subscribable_name)
)
```

`opened` is the timestamp of the first assignment to the collection.
`completed` is the timestamp of when the collection was marked "completed"

> [!NOTE]
> Completed assignable could just be cleaned away, but for now we keep exactly
> one completed item per subscription around, on the theory that some `SubscriptionManager`
> might require information about the prior collection it generated.
>
> This adds a bit of complexity, and I'm not sure it will be useful. In future, we
> may omit the `completed` field, and completed assignables will simply be
> deleted, and any items whose assignable have all been deleted will be immediately
> cleared.

##### assignment (an item in a collection)

Next there is `assignment`, which represents an item in an `assignable`, i.e. a collection.
It's pretty self-explanatory I think.

```sql
CREATE TABLE assignment(
  subscribable_name VARCHAR(64),
  within_type_id    VARCHAR(1024),
  guid              VARCHAR(1024),
  PRIMARY KEY( subscribable_name, within_type_id, guid ),
  FOREIGN KEY( subscribable_name, within_type_id ) REFERENCES assignable( subscribable_name, within_type_id )
)
```

##### subscription

Next there is `subscription`, which just maps a destination to a `subscribable`.
the destination is JSON blob that can refer to a variety of things: e-mail addresses, SMS numbers, mastodon instances, etc.
Each `SubscriptionManager` works with a destination subtype. 

```sql
CREATE TABLE subscription(
  subscription_id    BIGINT,
  destination_json   JSONB         NOT NULL,
  destination_unique VARCHAR(1024) NOT NULL,
  subscribable_name  VARCHAR(64)   NOT NULL,
  confirmed          BOOLEAN       NOT NULL,
  added              TIMESTAMP     NOT NULL,
  PRIMARY KEY( subscription_id ),
  FOREIGN KEY( subscribable_name ) REFERENCES subscribable( subscribable_name )
)
```

Since destinations can have ornamentation (an e-mail address,
for example, might have a personal part (e.g. Buffy in "Buffy <b@slayers.org>"), it's not sufficient to prevent multiple
subscriptions to insist that the JSON entities be unique. So destinations declare a unique core, whose uniqueness within
a subscription the database enforces:

```sql
CREATE UNIQUE INDEX destination_unique_subscribable_name ON subscription(destination_unique, subscribable_name)
```

That's it for the base schema! There are also tables that convert destinations specific to subscription
types into their various queues for notification. I'm omitting those for now.

