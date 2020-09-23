import copy
import uuid

from sqlalchemy import and_

from anchore_engine import db
from anchore_engine.db.entities.catalog import ImageInventory
from anchore_engine.db.entities.common import anchore_now
from anchore_engine.subsys import logger


def list_image_inventory_by_type(account: str, inventory_type: str, state=None, session=None):
    """
    Retrieve a list of ImageInventory dictionary objects, by type of inventory and account
    """
    if not session:
        session = db.Session

    ret = {}

    filter_args = [ImageInventory.account == account, ImageInventory.inventory_type == inventory_type]

    if state is not None:
        filter_args.append(ImageInventory.state == state)

    results = session.query(ImageInventory) \
        .filter(and_(*filter_args)) \
        .all()

    if results:
        for result in results:
            ret_obj = (dict((key, value) for key, value in vars(result).items() if not key.startswith('_')))
            ret[build_key(ret_obj)] = ret_obj
    logger.info(repr(ret))
    return ret


def build_key(inventory_image):
    """
    There needs to be a strong matching mechanism for determining if an image exists already or not
    (without being able to use the ID)
    """
    return "{}:{}".format(inventory_image['image_tag'], inventory_image['image_repo_digest'])


def add_image_inventory(session=None, account=None, inventory_type=None, inventory=None):
    if not session:
        session = db.Session

    existing_inventory_images = copy.deepcopy(list_image_inventory_by_type(account, inventory_type, session))

    image_inventory = {
        "inventory_type": inventory_type,
        "account": account
    }
    # For images that don't exist in the database, insert. For images that do, update their last_updated.
    for inventory_record in inventory:
        image_inventory.update(inventory_record)
        image_inventory['id'] = str(uuid.uuid4())

        image_lookup_key = build_key(inventory_record)
        if image_lookup_key in existing_inventory_images:
            # In this case, we just want to update last_updated to reflect this report of the inventory
            existing_image = existing_inventory_images[image_lookup_key]

            # image_inventory's last_updated field equals the timestamp sent from KAI
            update_last_updated(existing_image.get('id', ''), image_inventory.get('last_updated', anchore_now()))
            del existing_inventory_images[image_lookup_key]
        else:
            # New record, we should insert
            inventory = ImageInventory(**image_inventory)
            session.add(inventory)

    # For images that exist in the db as active, but are not in the updated inventory, we should update their state
    # to "inactive"
    for key, value in existing_inventory_images:
        if value.state == 'active':
            update_state_to_inactive(value.get('id', ''), session)

    return True


def update_last_updated(inventory_id, last_updated, session=None):
    if not session:
        session = db.Session()

    record = session.query(ImageInventory).filter_by(id=inventory_id).first()

    if record:
        record.last_updated = last_updated


def update_state_to_inactive(inventory_id: str, session=None):
    if not session:
        session = db.Session()

    active_record = session.query(ImageInventory).filter_by(id=inventory_id, state='active').first()

    if active_record:
        active_record.state = 'inactive'
