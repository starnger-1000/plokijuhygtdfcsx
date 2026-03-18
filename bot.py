# bot.py
# Full Club Auction & Pokémon Shop Bot (Certified Final Production Build v5.8)
# Part 1 of 4 - Core, Config, Events, Economy & Groups
# Dependencies: discord.py, pymongo, dnspython, certifi

import os
import asyncio
import re
import random
import string
from datetime import datetime, timedelta
import discord
from discord.ext import commands
from discord.ui import View, Button, Select
from pymongo import MongoClient, ReturnDocument
import certifi
from fastapi import FastAPI
import uvicorn
import threading
import typing
from discord.ext import tasks
import re
from datetime import datetime, timezone, timedelta
import math
import chat_exporter
import io

# ---------- CONFIGURATION ----------
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID")) if os.getenv("BOT_OWNER_ID") else None

# Channel IDs
LOG_CHANNELS = {
    "withdraw": 1443955732281167873, 
    "battle": 1439844034905374720,
    "club": 1443955856222851142,
    "duelist": 1443955967086690395,
    "shop_log": 1446017729340379246, # Pokemon Confirmation Deal Embed Message
    "shop_main": 1446018190093058222, # Shop interface
    "login_log": 1455496870003740736,  # Fixed: Changed from Variable = Value to "key": Value
    "chat_channel": 975275349573271552 # Daily Task Channel
}

# Constants
TIME_LIMIT = 90 
MIN_INCREMENT_PERCENT = 5
LEAVE_PENALTY_PERCENT = 10
DUELIST_MISS_PENALTY_PERCENT = 15
WIN_VALUE_BONUS = 100000
LOSS_VALUE_PENALTY = -100000
OWNER_MSG_VALUE_BONUS = 10000
OWNER_MSG_COUNT_PER_BONUS = 100
DEFAULT_TAX_RATE = 0.05 
TAX_BUYER_ADD = 0.025 # 2.5% Extra paid by buyer
TAX_SELLER_SUB = 0.025 # 2.5% Deducted from seller
DAILY_MSG_REQ = 100

# Configuration
SERVER_TAG_STRING = "『 𝐙𝐄 』" # The exact text to look for
TAG_BOX_NAME = "Tag Mystery Box"

LEVEL_UP_CONFIG = [
    (12, "5th Division", 50000), (27, "4th Division", 100000), (45, "3rd Division", 150000),
    (66, "2nd Division", 200000), (90, "1st Division", 300000), (117, "17th Position", 320000),
    (147, "15th Position", 360000), (180, "12th Position", 400000), (216, "10th Position", 450000),
    (255, "8th Position", 500000), (297, "6th Position", 550000), (342, "Conference League", 600000),
    (390, "5th Position", 650000), (441, "Europa League", 700000), (495, "4th Position", 750000),
    (552, "3rd Position", 800000), (612, "Champions League", 900000), (675, "2nd Position", 950000),
    (741, "1st Position and League Winner", 1000000), (810, "UCL Winner", 1500000), (882, "Treble Winner", 2000000),
]

# ==============================================================================
#  CHAT LEVELING SYSTEM CONFIG & HELPER
# ==============================================================================

LEVEL_REWARDS = {
    5: {"pc": 100000, "cash": 200000},
    10: {"pc": 300000, "cash": 400000},
    15: {"pc": 600000, "cash": 800000},
    20: {"pc": 900000, "cash": 1200000},
    25: {"pc": 1200000, "cash": 1500000},
    30: {"pc": 1500000, "cash": 1800000},
    40: {"pc": 2200000, "cash": 2000000},
    50: {"pc": 3000000, "cash": 3000000},
    60: {"pc": 4000000, "cash": 4000000},
    70: {"pc": 5000000, "cash": 5000000},
    80: {"pc": 6000000, "cash": 6000000},
    90: {"pc": 7000000, "cash": 7000000},
    100: {"pc": 8500000, "cash": 8000000},
    120: {"pc": 10000000, "cash": 9000000},
    150: {"pc": 15000000, "cash": 10000000},
    180: {"pc": 20000000, "cash": 20000000},
    200: {"pc": 30000000, "cash": 30000000}
}

def calc_level_data(total_msgs):
    """Calculates Current Level, Progress, and Target based on total lifetime msgs."""
    reqs = [50, 100, 200, 400, 800]
    level = 0
    msgs_left = total_msgs
    
    for r in reqs:
        if msgs_left >= r:
            level += 1
            msgs_left -= r
        else:
            return level, msgs_left, r
            
    # From Level 5 onwards, it's a flat 1000 msgs per level
    level += msgs_left // 1000
    rem = msgs_left % 1000
    return level, rem, 1000

# Emojis
E_PC = "<:pokecoins:1446019648901484616>"
E_SHINY = "<a:poke_coin:1446005721370984470>"
E_ACTIVE = "<a:geeen_dot:1443252917648752681>"
E_DANGER = "<a:red_dot:1443261605092786188>"
E_ALERT = "<a:alert:1443254143308533863>" 
E_ERROR = "<a:cross2:972155180185452544>"
E_SUCCESS = "<a:verified:962942818886770688>"
E_GOLD_TICK = "<a:goldcheckmark:1443253229398917252>"
E_CROWN = "<a:crownop:962190451744579605>"
E_ADMIN = "<a:HeadAdmin_red:1443253359095058533>"
E_PREMIUM = "<a:donate_red:1443252440634884117>"
E_BOOST = "<a:boost:962277213204525086>"
E_PIKACHU = "<a:miapikachu:1443253477533814865>"
E_MONEY = "<a:Donation:962944611792326697>"
E_TIMER = "<a:1031pixelclock:1443253900793741332>"
E_STARS = "<a:bluestars:1443254349869486140>"
E_ITEMBOX = "<a:itembox:1443254784898367629>" 
E_FIRE = "<a:redfire1:1443251827490684938>"
E_ARROW = "<a:arrow_arrow:962945777821450270>"
E_RED_ARROW = "<a:redarrow:1443251741905653811>"
E_STAR = "<a:yellowstar:1443252221645950996>"
E_AUCTION = "<:Auction:1443250889266565190>"
E_BOOK = "<a:rules:1443252031220613321>"
E_NYAN = "<a:NyanCat:1443253686771126454>"
E_GIVEAWAY = "<a:gw:1443251079705001984>" 
E_CHAT = "<a:text:1443251311939293267>"

BATTLE_BANTER = [
    "<a:redfire1:1443251827490684938> Absolute demolition! **{winner}** tore **{loser}** apart. {l_emoji}",
    "<a:miapikachu:1443253477533814865> **{winner}** owned the pitch today! {l_emoji} <:e:1443996214805790871>",
    "<a:cross2:972155180185452544> That was a public execution. RIP **{loser}**. {w_emoji}",
    "<a:crownop:962190451744579605> **{winner}** delivered a masterclass. {w_emoji}"
]
WINNER_REACTIONS = [":7833dakorcalmao:", ":33730ohoholaugh:", "1443996271990935552", "1443996171071914177"]
LOSER_REACTIONS = ["<:192978sadchinareact:1443996152772038678>", "1443996269113643028", "1443996139362844783"]

# === TROPHY & AWARD EMOJIS ===
E_UCL = "<:ucl:1482846831598637098>"
E_LEAGUE = "<:league:1482846958941900890>"
E_SUPERCUP = "<:supercup:1482847038310711357>"
E_BALLONDOR = "<:balondor:1482847124562640967>"
E_SUPERBALLONDOR = "<:superbalondor:1482847189515374702>"

DONOR_WEIGHTS = {
    972809181444861984: 1, 972809182224994354: 1, 972809183374225478: 2,
    972809180966703176: 2, 972809183718150144: 4, 972809184242434048: 8,
    973502021757968414: 12
}

# ---------- DATABASE ----------
if not MONGO_URL:
    print("CRITICAL: MONGO_URL missing.")
    db = None
else:
    cluster = MongoClient(MONGO_URL, tlsCAFile=certifi.where())
    db = cluster["auction_bot"]

if db is not None:
    clubs_col = db.clubs
    duelists_col = db.duelists
    groups_col = db.investor_groups
    group_members_col = db.groups_members
    wallets_col = db.personal_wallets
    profiles_col = db.user_profiles
    bids_col = db.bids
    history_col = db.club_history
    contracts_col = db.duelist_contracts
    battles_col = db.battle_register
    audit_col = db.audit_logs
    config_col = db.bot_config
    counters_col = db.counters
    past_entities_col = db.past_entities
    activities_col = db.user_activities
    pending_deals_col = db.pending_deals # For Club Buying
    shop_items_col = db.shop_items
    box_limits_col = db.box_limits # NEW: Stores user purchase limits
    inventory_col = db.inventory
    coupons_col = db.coupons
    redeem_codes_col = db.redeem_codes
    message_counts_col = db.message_counts
    pending_shop_approvals = db.pending_shop_approvals # New collection for Shop Approvals
    quests_col = db.quests
    contracts_col = db.contracts
    pending_contracts_col = db.pending_contracts
    pending_transfers_col = db.pending_transfers
    deposits_col = db.deposits
    auction_schedules_col = db.auction_schedules
    auction_queue_col = db.auction_queue
    auction_stats_col = db.auction_stats
    auction_history_col = db.auction_history

# Indian Standard Time (IST) setup
IST = timezone(timedelta(hours=5, minutes=30))
    
def get_next_id(sequence_name):
    if db is None: return 0
    ret = counters_col.find_one_and_update(
        {"_id": sequence_name}, {"$inc": {"seq": 1}},
        upsert=True, return_document=ReturnDocument.AFTER
    )
    return ret['seq']

# ---------- BOT SETUP ----------
# ---------- BOT SETUP & HELPERS ----------
# Global variable to store prefix in memory
cached_prefix = "." 

def get_prefix(bot, message):
    # Read from memory instantly instead of waiting for database
    return cached_prefix

# ... (Intents and bot init remain same) ...

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)
active_timers = {}
bidding_frozen = False

# ---------- HELPER CLASSES ----------
def get_wallet(user_id):
    """Smart Wallet Fetcher (Handles String/Int IDs & Syncs)"""
    if db is None: return None
    # 1. Try Finding by String ID (Standard)
    w = wallets_col.find_one({"user_id": str(user_id)})
    if w: return w
    # 2. Try Finding by Integer ID (Legacy)
    w = wallets_col.find_one({"user_id": int(user_id)})
    if w:
        # Auto-Migrate to String
        wallets_col.update_one({"_id": w["_id"]}, {"$set": {"user_id": str(user_id)}})
        return w
    # 3. Create if missing (Default)
    new_wallet = {"user_id": str(user_id), "balance": 0, "shiny_coins": 0, "pc": 0}
    wallets_col.insert_one(new_wallet)
    return new_wallet
class HumanInt(commands.Converter):
    async def convert(self, ctx, argument):
        try:
            clean = argument.lower().replace(",", "").replace("$", "")
            if "k" in clean: return int(float(clean.replace("k", "")) * 1000)
            if "m" in clean: return int(float(clean.replace("m", "")) * 1000000)
            if "b" in clean: return int(float(clean.replace("b", "")) * 1000000000)
            return int(clean)
        except: raise commands.BadArgument(f"Invalid number: {argument}")

class DepositModal(discord.ui.Modal, title="Deposit Pokécoins"):
    amount = discord.ui.TextInput(
        label="Amount of PC to Deposit", 
        placeholder="e.g. 150000", 
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pc_amount = int(self.amount.value.replace(",", ""))
        except ValueError:
            return await interaction.response.send_message(embed=create_embed("Invalid", f"{E_ERROR} Please enter a valid number.", 0xff0000), ephemeral=True)

        if pc_amount <= 0:
            return await interaction.response.send_message(embed=create_embed("Invalid", f"{E_ERROR} Amount must be greater than 0.", 0xff0000), ephemeral=True)

        # Generate custom ID (dpc1, dpc2, etc.)
        deposit_count = deposits_col.count_documents({}) + 1
        dep_id = f"dpc{deposit_count}"

        # Save to Database
        new_deposit = {
            "deposit_id": dep_id,
            "user_id": str(interaction.user.id),
            "amount": pc_amount,
            "status": "Queued",
            "market_id": None,
            "created_at": datetime.now(timezone.utc),
            "listed_at": None
        }
        deposits_col.insert_one(new_deposit)

        await interaction.response.send_message(embed=create_embed("Deposit Queued", f"{E_SUCCESS} Deposit request for **{pc_amount:,} PC** queued (ID: `{dep_id}`).\n\nPlease wait in your DMs for the Market ID.", 0x2ecc71), ephemeral=True)

        # Send command to the market channel
        market_channel = interaction.client.get_channel(1483437925139218613)
        if market_channel:
            await market_channel.send(f"&say <@716390085896962058> m a l {pc_amount}")

class DepositView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Deposit PC", style=discord.ButtonStyle.green, custom_id="dep_pc_btn")
    async def deposit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DepositModal())

# ==========================================
# 🛑 POKEMON AUCTION SYSTEM
# ==========================================

class AuctionInfoView(discord.ui.View):
    def __init__(self, guild_id):
        super().__init__(timeout=None)
        # Link button directly to the Registration Channel
        url = f"https://discord.com/channels/{guild_id}/1483860214854844476"
        self.add_item(discord.ui.Button(label="Go to Registration Channel", url=url, style=discord.ButtonStyle.link))

class AuctionVoteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=15.0) # 15 second voting timer
        self.yes_votes = set()
        self.no_votes = set()

    @discord.ui.button(label="Yes, proceed", style=discord.ButtonStyle.green)
    async def vote_yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.yes_votes.add(interaction.user.id)
        self.no_votes.discard(interaction.user.id)
        await interaction.response.send_message(f"{E_SUCCESS} Vote cast: YES", ephemeral=True)

    @discord.ui.button(label="No, skip", style=discord.ButtonStyle.danger)
    async def vote_no(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.no_votes.add(interaction.user.id)
        self.yes_votes.discard(interaction.user.id)
        await interaction.response.send_message(f"{E_ERROR} Vote cast: NO", ephemeral=True)
        
@bot.listen('on_message')
async def auction_registration_scanner(message):
    # Ignore bots
    if message.author.bot:
        return

# Ban Role Check
    if discord.utils.get(message.author.roles, id=1483904292078227707):
        return # Silently ignore banned users
    
    # Only listen in the Registration Channel
    if message.channel.id == 1483860214854844476:
        
        # Only process if the registration window is officially OPEN
        if getattr(bot, 'registration_active', False) is False:
            return

        # Check if the message matches: @PokéTwo i <number>
        # ID for PokéTwo is 716390085896962058
        match = re.match(r'^<@!?716390085896962058>\s+i\s+(\d+)$', message.content.strip(), re.IGNORECASE)
        
        if match:
            pokemon_id = match.group(1)
            user_id = message.author.id

            # 1. Check Server Limit (Max 25 total)
            total_entries = auction_queue_col.count_documents({})
            if total_entries >= 25:
                return # Block is full, silently ignore
            
            # 2. Check User Limit (Max 2 per user)
            user_entries = auction_queue_col.count_documents({"user_id": user_id})
            if user_entries >= 2:
                return # User hit their limit, silently ignore

            # 3. Success! Save to Database as Auc1, Auc2, etc.
            auc_id = f"Auc{total_entries + 1}"
            auction_queue_col.insert_one({
                "auction_id": auc_id,
                "pokemon_id": pokemon_id,
                "user_id": user_id,
                "status": "queued"
            })
            
async def run_live_auction(bot, guild):
    bidding_channel = guild.get_channel(1483860258932916336)
    disputes_channel = guild.get_channel(1483860590907883580)
    seller_role = guild.get_role(1483871103473553498)
    
    # Get all queued auctions
    queue = list(auction_queue_col.find({"status": "queued"}))
    total_slots = len(queue)
    
    if total_slots == 0:
        return await bidding_channel.send(embed=create_embed("Auction Canceled", f"{E_ERROR} No Pokémon were registered today!", 0xff0000))

    await bidding_channel.send(embed=create_embed("Live Auction Starting", f"{E_SUCCESS} The floor is open! We have **{total_slots}** Pokémon on the block today.", 0x2ecc71))

    for index, item in enumerate(queue):
        auc_id = item["auction_id"]
        seller_id = item["user_id"]
        pokemon_id = item["pokemon_id"]
        seller = guild.get_member(seller_id)

        # --- PHASE 3A: THE SUMMON ---
        if seller:
            await seller.add_roles(seller_role)
        
        summon_desc = (
            f"**SELLER:** <@{seller_id}>\n\n"
            f"You are up! Please spawn your registered Pokémon for the server to review.\n"
            f"**Command:** `<@716390085896962058> i {pokemon_id}`\n\n"
            f"*(You have 90 seconds to do this, or your slot will be skipped and you will be fined 2,000 PC!)*"
        )
        await bidding_channel.send(content=f"<@{seller_id}>", embed=create_embed(f"{E_ALERT} AUCTION QUEUE: SLOT {index+1}/{total_slots} (ID: {auc_id})", summon_desc, 0x3498db))
        
        # Wait for the seller to type the info command
        def check_info(m):
            return m.author.id == seller_id and m.channel.id == bidding_channel.id and f"i {pokemon_id}" in m.content.lower()

        try:
            info_msg = await bot.wait_for('message', timeout=90.0, check=check_info)
        except asyncio.TimeoutError:
            # Trap 1: AFK Seller Penalty
            await bidding_channel.send(embed=create_embed("Dispute Triggered", f"{E_ERROR} Seller failed to info in 90s. Slot skipped.", 0xff0000))
            if seller: await seller.remove_roles(seller_role)
            db.wallets.update_one({"user_id": seller_id}, {"$inc": {"balance": -2000}}, upsert=True)
            await disputes_channel.send(embed=create_embed(f"{E_ERROR} DISPUTE LOG: AFK SELLER", f"**User:** <@{seller_id}>\n**ID:** {auc_id}\n**Penalty:** 2,000 PC deducted.", 0xff0000))
            continue

        await asyncio.sleep(2)

        # --- PHASE 3B: QUALITY CONTROL VOTE ---
        await bidding_channel.set_permissions(guild.default_role, send_messages=False)
        vote_view = AuctionVoteView()
        
        vote_desc = (
            f"{E_ALERT} **To ensure maximum quality, the community must verify this Pokémon.**\n\n"
            f"Please review the Pokémon above. Is this worth auctioning?\n"
            f"*(You have 15 seconds to vote.)*"
        )
        vote_msg = await bidding_channel.send(embed=create_embed(f"{E_ALERT} VOTING PHASE - {auc_id}", vote_desc, 0xe67e22), view=vote_view)
        
        await vote_view.wait()
        
        yes_count = len(vote_view.yes_votes)
        no_count = len(vote_view.no_votes)
        
        if no_count > yes_count:
            # Trap 2: Trash Registration Penalty
            await bidding_channel.send(embed=create_embed("Vote Failed", f"{E_ERROR} Community rejected this Pokémon. Slot skipped.", 0xff0000))
            if seller: await seller.remove_roles(seller_role)
            db.wallets.update_one({"user_id": seller_id}, {"$inc": {"balance": -2000}}, upsert=True)
            await disputes_channel.send(embed=create_embed(f"{E_ERROR} DISPUTE LOG: FAILED VOTE", f"**User:** <@{seller_id}>\n**ID:** {auc_id}\n**Votes:** {yes_count} Yes / {no_count} No\n**Penalty:** 2,000 PC deducted.", 0xff0000))
            continue

        # --- PHASE 3C: THE BIDDING WAR ---
        await bidding_channel.set_permissions(guild.default_role, send_messages=True)
        await bidding_channel.send(embed=create_embed("Vote Passed!", f"{E_SUCCESS} The floor is open! Start placing your bids (e.g., `10k`, `1m`).", 0x2ecc71))

        current_bid = 0
        highest_bidder = None
        min_increment = 0
        tracker_msg = None

        def check_bid(m):
            if m.channel.id != bidding_channel.id or m.author.bot or m.author.id == seller_id:
                return False
            
            content = m.content.lower().replace(",", "")
            if not (content.endswith('k') or content.endswith('m')):
                return False
                
            try:
                multiplier = 1000 if content.endswith('k') else 1000000
                bid_amount = int(float(content[:-1]) * multiplier)
            except ValueError:
                return False

            if bid_amount < min_increment or bid_amount <= current_bid:
                # Custom error reaction instead of default cross
                bot.loop.create_task(m.add_reaction(E_ERROR))
                return False

            # Ghost Bank Check
            user_wallet = db.wallets.find_one({"user_id": m.author.id})
            balance = user_wallet.get("balance", 0) if user_wallet else 0
            
            if balance < bid_amount:
                return False 
                
            m.valid_bid_amount = bid_amount
            return True

        # The Timer Loop
        bidding_active = True
        while bidding_active:
            try:
                # 30 Second Timer
                bid_msg = await bot.wait_for('message', timeout=30.0, check=check_bid)
                
                current_bid = bid_msg.valid_bid_amount
                highest_bidder = bid_msg.author.id
                min_increment = int(current_bid * 1.025)

                # Quest Hook: Add 1 valid bid to their profile
                auction_stats_col.update_one({"user_id": highest_bidder}, {"$inc": {"bids_made": 1}}, upsert=True)
                
                # Custom success reaction instead of default checkmark
                await bid_msg.add_reaction(E_SUCCESS)
                
                if tracker_msg:
                    try: await tracker_msg.delete()
                    except: pass
                    
                track_desc = f"{E_MONEY} **HIGHEST BID:** {current_bid:,} PC (<@{highest_bidder}>)\n\n{E_ALERT} **Next Minimum Bid:** `{min_increment:,} PC` *(+2.5%)*"
                tracker_msg = await bidding_channel.send(embed=create_embed("Live Bid Tracker", track_desc, 0x3498db))
                
            except asyncio.TimeoutError:
                if current_bid == 0:
                    await bidding_channel.send(embed=create_embed("No Bids", f"{E_ALERT} No one bid on {auc_id}. Moving to next slot.", 0x95a5a6))
                    bidding_active = False
                    break
                    
                # 15 Second Warning
                warn_desc = f"**<@{highest_bidder}>** holds the highest bid at **{current_bid:,} PC**!\nIf no higher bids are placed in the next **15 seconds**, the auction will close!"
                await bidding_channel.send(embed=create_embed(f"{E_ALERT} GOING ONCE...", warn_desc, 0xe67e22))
                
                try:
                    bid_msg = await bot.wait_for('message', timeout=15.0, check=check_bid)
                    current_bid = bid_msg.valid_bid_amount
                    highest_bidder = bid_msg.author.id
                    min_increment = int(current_bid * 1.025)
                    
                    await bid_msg.add_reaction(E_SUCCESS)
                    
                    if tracker_msg:
                        try: await tracker_msg.delete()
                        except: pass
                        
                    track_desc = f"{E_MONEY} **HIGHEST BID:** {current_bid:,} PC (<@{highest_bidder}>)\n\n{E_ALERT} **Next Minimum Bid:** `{min_increment:,} PC` *(+2.5%)*"
                    tracker_msg = await bidding_channel.send(embed=create_embed("Live Bid Tracker", track_desc, 0x3498db))
                    
                except asyncio.TimeoutError:
                    await bidding_channel.send(embed=create_embed(f"{E_SUCCESS} SOLD!", f"Congratulations to <@{highest_bidder}> for winning **{auc_id}** for **{current_bid:,} PC**!", 0x2ecc71))
                    bidding_active = False
                    
                    # Transition to Phase 4 (Escrow thread)
                    await create_escrow_thread(bot, guild, auc_id, seller_id, highest_bidder, current_bid, pokemon_id)

        # Clean up role for the next iteration
        if seller: await seller.remove_roles(seller_role)
        await bidding_channel.purge(limit=100)
        await asyncio.sleep(5)

async def create_escrow_thread(bot, guild, auc_id, seller_id, buyer_id, final_price, pokemon_id):
    bidding_channel = guild.get_channel(1483860258932916336)
    accept_logs = guild.get_channel(1483860540840214629)
    disputes_logs = guild.get_channel(1483860590907883580)
    typing_role = guild.get_role(1483871103473553498)

    seller = guild.get_member(seller_id)
    buyer = guild.get_member(buyer_id)

    # 1. CREATE THE PRIVATE ESCROW THREAD
    thread = await bidding_channel.create_thread(
        name=f"Escrow: {auc_id}",
        type=discord.ChannelType.private_thread,
        invitable=False
    )
    
    # 2. PULL USERS IN & ASSIGN ROLES
    if seller:
        await thread.add_user(seller)
        await seller.add_roles(typing_role)
    if buyer:
        await thread.add_user(buyer)
        await buyer.add_roles(typing_role)

    desc = (
        f"**Seller:** <@{seller_id}>\n"
        f"**Buyer:** <@{buyer_id}>\n"
        f"**Final Price:** {final_price:,} PC\n\n"
        f"{E_ALERT} **Instructions for Seller:**\n"
        f"1. Start trade: `<@716390085896962058> t <@{buyer_id}>`\n"
        f"2. Add Pokémon: `<@716390085896962058> t a {pokemon_id}`\n"
        f"3. Both confirm: `<@716390085896962058> t c`\n\n"
        f"{E_MONEY} *Ze Bot will automatically deduct the PC from the Buyer and send it to the Seller once PokéTwo confirms. Do NOT add PC to the trade!*"
    )
    await thread.send(content=f"<@{seller_id}> <@{buyer_id}>", embed=create_embed(f"💼 OFFICIAL TRADE ROOM - {auc_id}", desc, 0x3498db))

    def check_trade(m):
        return m.channel.id == thread.id

    trade_active = True
    dispute_triggered = False
    dispute_reason = ""
    offender_id = None

    # The 5-Minute Maximum Escrow Timer
    end_time = asyncio.get_event_loop().time() + 300.0 

    # 3. THE "EYE IN THE SKY" (Watching the chat)
    while trade_active:
        timeout_left = end_time - asyncio.get_event_loop().time()
        if timeout_left <= 0:
            # Trap 5: 5-Minute Time Waster (Buyer's fault for not confirming)
            dispute_triggered = True
            dispute_reason = "Trade Timeout (5 Minutes passed without confirmation)"
            offender_id = buyer_id 
            break

        try:
            msg = await bot.wait_for('message', timeout=timeout_left, check=check_trade)
            content = msg.content.lower()

            # Trap 3: Seller tries to Bait & Switch
            if msg.author.id == seller_id and f"t r {pokemon_id}" in content:
                dispute_triggered = True
                dispute_reason = f"Bait & Switch (Removed Registered Pokémon {pokemon_id})"
                offender_id = seller_id
                break

            # Trap 4: Someone manually aborts the trade
            if "t x" in content and msg.author.id in [seller_id, buyer_id]:
                dispute_triggered = True
                dispute_reason = "Trade Aborted Manually"
                offender_id = msg.author.id
                break

            # SUCCESS: PokéTwo confirms the trade is executing!
            if msg.author.id == 716390085896962058 and "executing" in content:
                trade_active = False
                break

        except asyncio.TimeoutError:
            dispute_triggered = True
            dispute_reason = "Trade Timeout (5 Minutes passed without confirmation)"
            offender_id = buyer_id
            break

    # 4. GENERATE HTML TRANSCRIPT
    transcript = await chat_exporter.export(thread)
    transcript_file = discord.File(io.BytesIO(transcript.encode()), filename=f"transcript_{auc_id}.html")
    
# 5. RESOLUTION
    # Send the log first so we can grab the message URL for the Admin HTML button!
    if dispute_triggered:
        log_msg = await disputes_logs.send(embed=create_embed(f"{E_ERROR} DISPUTE LOG", f"**User:** <@{offender_id}>\n**ID:** {auc_id}\n**Reason:** {dispute_reason}", 0xff0000), file=transcript_file)
        await thread.send(embed=create_embed("Dispute Triggered", f"{E_ERROR} Trade failed: {dispute_reason}. Thread locking.", 0xff0000))
        
        # Stat updates for Disputer
        db.wallets.update_one({"user_id": offender_id}, {"$inc": {"balance": -2000}}, upsert=True)
        auction_stats_col.update_one({"user_id": offender_id}, {"$inc": {"disputes_caused": 1, "penalties_paid": 2000}}, upsert=True)
        
        # Save to History for .aucinfo
        auction_history_col.insert_one({
            "auction_id": auc_id, "seller_id": seller_id, "buyer_id": buyer_id, "pokemon_id": pokemon_id, 
            "status": "Disputed", "dispute_reason": dispute_reason, "log_url": log_msg.jump_url
        })
    else:
        log_msg = await accept_logs.send(embed=create_embed(f"RECEIPT: {auc_id}", f"**Seller:** <@{seller_id}>\n**Buyer:** <@{buyer_id}>\n**Price:** {final_price:,} PC", 0x2ecc71), file=transcript_file)
        await thread.send(embed=create_embed("Trade Confirmed", f"{E_SUCCESS} Ze Bot successfully transferred {final_price:,} PC!", 0x2ecc71))
        
        # Stat updates for Buyer and Seller (Quests!)
        db.wallets.update_one({"user_id": buyer_id}, {"$inc": {"balance": -final_price}})
        db.wallets.update_one({"user_id": seller_id}, {"$inc": {"balance": final_price}}, upsert=True)
        auction_stats_col.update_one({"user_id": buyer_id}, {"$inc": {"pc_spent": final_price, "auctions_won": 1}}, upsert=True)
        auction_stats_col.update_one({"user_id": seller_id}, {"$inc": {"pc_earned": final_price, "confirmed_trades": 1, "pokemon_registered": 1}}, upsert=True)
        
        # Save to History for .aucinfo
        auction_history_col.insert_one({
            "auction_id": auc_id, "seller_id": seller_id, "buyer_id": buyer_id, "pokemon_id": pokemon_id,
            "final_price": final_price, "status": "Confirmed", "log_url": log_msg.jump_url
        })
    
    # 6. CLEANUP & LOCKDOWN
    if seller: await seller.remove_roles(typing_role)
    if buyer: await buyer.remove_roles(typing_role)
    await asyncio.sleep(3) # Give Discord a second to process
    await thread.edit(archived=True, locked=True)
        
async def execute_auction_protocol(bot):
    guild = bot.guilds[0]
    info_channel = guild.get_channel(1483860096415961188)
    reg_channel = guild.get_channel(1483860214854844476)
    
    if not info_channel or not reg_channel:
        return print("[AUCTION ERROR] Channels not found!")

    # 0. PREP: Wipe the old queue from yesterday so we start fresh!
    auction_queue_col.delete_many({})

    # 1. THE ANNOUNCEMENT
    desc = (
        f"Welcome to the Ze Bot Premium Auction! The automated protocol has been engaged.\n\n"
        f"{E_SUCCESS} **1. Registration**\n"
        f"Want to sell a Pokémon? Click the button below to head to <#1483860214854844476>. "
        f"The gates are locked, but will open in exactly **90 Seconds**!\n\n"
        f"{E_ALERT} **2. Live Bidding**\n"
        f"Once registered, all bidding happens in <#1483860258932916336>.\n\n"
        f"{E_MONEY} **3. Bank Check**\n"
        f"Ensure you have used `.depositpc`! You cannot bid what you don't have."
    )
    await info_channel.send(content="<@&1442917733422465024>")
    await info_channel.send(embed=create_embed(f"{E_ALERT} THE POKÉMON AUCTION IS STARTING!", desc, 0xe67e22), view=AuctionInfoView(guild.id))

    # 2. WAIT 90 SECONDS
    await asyncio.sleep(90)

    # 3. UNLOCK GATES & TURN SCANNER ON
    bot.registration_active = True
    await reg_channel.set_permissions(guild.default_role, send_messages=True)
    
    reg_desc = (
        f"The gates are unlocked! You have exactly **5 Minutes** to submit your Pokémon.\n\n"
        f"**How to submit:**\n"
        f"Ping PokéTwo and type `i` followed by your Pokémon's ID.\n"
        f"*Example:* `<@716390085896962058> i 2132`\n\n"
        f"{E_ALERT} **The Rules:**\n"
        f"▫️ Max **2 Pokémon** per user.\n"
        f"▫️ The block only holds **25 Pokémon total**!"
    )
    await reg_channel.send(embed=create_embed(f"{E_SUCCESS} AUCTION REGISTRATION IS OPEN!", reg_desc, 0x2ecc71))

    # 4. WAIT 5 MINUTES FOR REGISTRATION
    await asyncio.sleep(300)

    # 5. LOCK GATES & TURN SCANNER OFF
    bot.registration_active = False
    await reg_channel.set_permissions(guild.default_role, send_messages=False)
    
    # Check exactly how many Pokémon were successfully captured by the scanner
    total_registered = auction_queue_col.count_documents({})
    
    lock_desc = (
        f"The auction block is fully loaded and locked. No further entries will be accepted.\n\n"
        f"📊 **Final Tally:** **{total_registered}/25** Pokémon successfully registered!\n\n"
        f"The live bidding is about to begin. Grab your wallets and head to <#1483860258932916336>!"
    )
    await reg_channel.send(embed=create_embed(f"{E_ERROR} REGISTRATION CLOSED!", lock_desc, 0xff0000))
    # Phase 3: Start the Live Bidding Engine!
    bot.loop.create_task(run_live_auction(bot, guild))
    
    # (Phase 3: The Live Bidding Summon will trigger right here!)

# Helper to parse time strings
def parse_time(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M").strftime("%H:%M")
    except ValueError:
        try:
            return datetime.strptime(time_str, "%I:%M %p").strftime("%H:%M")
        except ValueError:
            return None

@bot.command(name="scheduleauctions", aliases=["sauc"], description="Schedule a Pokémon auction (e.g., 14:30 or 2:30 PM).")
@commands.has_permissions(administrator=True)
async def scheduleauctions(ctx, *, time_str: str):
    parsed_time = parse_time(time_str)
    if not parsed_time:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Invalid format! Use `HH:MM` or `HH:MM PM`.", 0xff0000))

    if auction_schedules_col.count_documents({}) >= 6:
        return await ctx.send(embed=create_embed("Limit Reached", f"{E_ERROR} You already have 6 schedules active.", 0xff0000))

    if auction_schedules_col.find_one({"time": parsed_time}):
        return await ctx.send(embed=create_embed("Duplicate", f"{E_ERROR} An auction is already scheduled for this time.", 0xff0000))

    auction_schedules_col.insert_one({"time": parsed_time})
    
    # Build list of current schedules
    times = sorted([doc["time"] for doc in auction_schedules_col.find()])
    schedule_list = "\n".join([f"▫️ {datetime.strptime(t, '%H:%M').strftime('%I:%M %p')}" for t in times])
    
    desc = f"{E_SUCCESS} Auction scheduled daily at **{datetime.strptime(parsed_time, '%H:%M').strftime('%I:%M %p')} (IST)**.\n\n**Current Schedule:**\n{schedule_list}"
    await ctx.send(embed=create_embed("Auction Scheduled", desc, 0x2ecc71))

@bot.command(name="removescheduleauction", aliases=["rsauc"], description="Remove a scheduled auction time.")
@commands.has_permissions(administrator=True)
async def removescheduleauction(ctx, *, time_str: str):
    parsed_time = parse_time(time_str)
    result = auction_schedules_col.delete_one({"time": parsed_time})
    
    if result.deleted_count == 0:
        return await ctx.send(embed=create_embed("Not Found", f"{E_ERROR} No schedule found for that time.", 0xff0000))
        
    await ctx.send(embed=create_embed("Schedule Removed", f"{E_SUCCESS} Successfully removed the **{time_str}** auction slot.", 0x2ecc71))

@botcommand(name="forcepokemonauction", aliases=["fpa"], description="Instantly force start an auction protocol.")
@commands.has_permissions(administrator=True)
async def forcepokemonauction(ctx):
    desc = f"{E_ALERT} **Auction Forced!**\nProtocol engaged. The auction ping has been dispatched to <#1483860096415961188>."
    await ctx.send(embed=create_embed("Force Start", desc, 0x9b59b6))
    
    # Run the massive background protocol without blocking the bot
    bot.loop.create_task(execute_auction_protocol(bot))

# ==========================================================
# ⏰ THE AUTOMATED BACKGROUND CLOCK (Checks every 1 minute)
# ==========================================================
@tasks.loop(minutes=1)
async def auction_clock():
    now_ist = datetime.now(IST).strftime("%H:%M")
    
    # Check if the exact current minute matches any saved schedule
    if auction_schedules_col.find_one({"time": now_ist}):
        print(f"[AUCTION] Scheduled time {now_ist} hit! Starting protocol...")
        bot.loop.create_task(execute_auction_protocol(bot))

@auction_clock.before_loop
async def before_auction_clock():
    await bot.wait_until_ready()

# ==========================================================
# 📊 AUCTION STATS & UTILITY COMMANDS
# ==========================================================

@bot.command(name="auctionrules", aliases=["aucrule", "arule"], description="View the official Ze Bot Auction rules.")
async def auctionrules(ctx):
    desc = (
        f"{E_ALERT} **OFFICIAL AUCTION RULES**\n\n"
        f"**1. Registration:** Max 2 Pokémon per user. 25 slots total per auction.\n"
        f"**2. Funds:** You must `.depositpc` before bidding. Fake bids are ignored.\n"
        f"**3. The 2,000 PC Penalty:** You will be heavily fined if you:\n"
        f"- Fail to info your Pokémon within 90s.\n"
        f"- Submit a trash Pokémon that the community votes NO on.\n"
        f"- Try to swap/remove the registered Pokémon from the trade.\n"
        f"- Cancel the final trade or waste the 5-minute escrow timer."
    )
    await ctx.send(embed=create_embed("Ze Bot Premium Auctions", desc, 0x3498db))


@bot.command(name="auctionprofile", aliases=["aucp"], description="View your or another user's auction stats.")
async def auctionprofile(ctx, member: discord.Member = None):
    member = member or ctx.author
    stats = auction_stats_col.find_one({"user_id": member.id}) or {}

    desc = (
        f"**The Hustler Stats**\n"
        f"{E_MONEY} Total PC Earned: **{stats.get('pc_earned', 0):,} PC**\n"
        f"{E_MONEY} Total PC Spent: **{stats.get('pc_spent', 0):,} PC**\n"
        f"{E_ALERT} Most Expensive Auction: **{stats.get('highest_auc_id', 'None')}**\n\n"
        f"**Auction History**\n"
        f"- Bids Made: **{stats.get('bids_made', 0)}**\n"
        f"- Auctions Won: **{stats.get('auctions_won', 0)}** | Lost: **{stats.get('auctions_lost', 0)}**\n"
        f"- Pokémon Registered: **{stats.get('pokemon_registered', 0)}**\n"
        f"- Confirmed Trades: **{stats.get('confirmed_trades', 0)}**\n"
        f"- Disputes Caused: **{stats.get('disputes_caused', 0)}**\n"
        f"- Penalties Paid: **{stats.get('penalties_paid', 0):,} PC**"
    )
    await ctx.send(embed=create_embed(f"AUCTION PROFILE: {member.display_name}", desc, 0x9b59b6))


@bot.command(name="auctionleaderboard", aliases=["auclb"], description="View the top auction bidders and sellers.")
async def auctionleaderboard(ctx):
    # Get top 5 spenders
    top_buyers = list(auction_stats_col.find().sort("pc_spent", -1).limit(5))
    # Get top 5 earners
    top_sellers = list(auction_stats_col.find().sort("pc_earned", -1).limit(5))

    desc = f"{E_MONEY} **Top Bidders (PC Spent)**\n"
    for i, user in enumerate(top_buyers):
        if user.get("pc_spent", 0) > 0:
            desc += f"{i+1}. <@{user['user_id']}> - {user.get('pc_spent'):,} PC\n"

    desc += f"\n{E_SUCCESS} **Top Sellers (PC Earned)**\n"
    for i, user in enumerate(top_sellers):
        if user.get("pc_earned", 0) > 0:
            desc += f"{i+1}. <@{user['user_id']}> - {user.get('pc_earned'):,} PC\n"

    await ctx.send(embed=create_embed("ZE BOT AUCTION LEADERBOARD", desc, 0xf1c40f))


@bot.command(name="auctionstatus", aliases=["as", "aucs"], description="Check the status of all current queued auctions.")
async def auctionstatus(ctx):
    queue = list(auction_queue_col.find({"status": "queued"}))
    if not queue:
        return await ctx.send(embed=create_embed("Auction Status", f"{E_ERROR} The auction block is currently empty.", 0xff0000))

    desc = ""
    for item in queue:
        desc += f"- **{item['auction_id']}**: <@{item['user_id']}> (Poké ID: {item['pokemon_id']})\n"
    
    await ctx.send(embed=create_embed("Current Auction Queue", desc, 0x3498db))


class AdminLogView(discord.ui.View):
    def __init__(self, log_url):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="View HTML Transcript", url=log_url, style=discord.ButtonStyle.link))

@bot.command(name="auctioninfo", aliases=["aucinfo"], description="Look up the receipt and details of a specific Auction ID.")
async def auctioninfo(ctx, auc_id: str, mode: str = "user"):
    history = auction_history_col.find_one({"auction_id": auc_id.capitalize()})
    if not history:
        return await ctx.send(embed=create_embed("Not Found", f"{E_ERROR} No records found for **{auc_id}**.", 0xff0000))

    status_icon = E_SUCCESS if history['status'] == 'Confirmed' else E_ERROR
    desc = (
        f"**Pokémon ID:** `{history.get('pokemon_id', 'N/A')}`\n"
        f"**Seller:** <@{history.get('seller_id', '0')}>\n"
        f"**Buyer/Winner:** <@{history.get('buyer_id', '0')}>\n"
        f"**Final Price:** {history.get('final_price', 0):,} PC\n"
        f"**Status:** {status_icon} {history['status']}\n"
    )
    
    if history.get('dispute_reason'):
        desc += f"\n{E_ALERT} **Dispute Reason:** {history['dispute_reason']}"

    # If Admin requests it, give them the HTML transcript link button
    if mode.lower() == "admin" and ctx.author.guild_permissions.administrator:
        log_url = history.get('log_url', 'https://discord.com')
        await ctx.send(embed=create_embed(f"AUCTION RECEIPT: {auc_id} [ADMIN]", desc, 0x9b59b6), view=AdminLogView(log_url))
    else:
        await ctx.send(embed=create_embed(f"AUCTION RECEIPT: {auc_id}", desc, 0x3498db))

# ==============================================================================
#  PC WITHDRAWAL SYSTEM
# ==============================================================================

PC_APPROVAL_CHANNEL_ID = 1455496870003740736
PC_PING_ROLE_ID = 947137512814567444

ROLE_REDUCTIONS_HOURS = {
    973502021757968414: 168, # 7 days (Instant)
    954242292129075220: 168, # Instant
    962278336799858729: 168, # Instant
    972809184242434048: 96,  # 4 days
    972809183718150144: 60,  # 60 hours
    972809183374225478: 48,  # 2 days
    972809182224994354: 36,  # 36 hours
    972809181444861984: 24,  # 1 day
    972809180966703176: 0    # No reduction
}

class PCWithdrawModal(discord.ui.Modal, title="PC Withdrawal Request"):
    amount_input = discord.ui.TextInput(label="Amount of PC to Withdraw", placeholder="e.g. 50000", style=discord.TextStyle.short, required=True)
    market_id_input = discord.ui.TextInput(label="Market ID of Pokémon", placeholder="e.g. 12345678", style=discord.TextStyle.short, required=True)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            amount = int(self.amount_input.value.replace(",", "").replace("k", "000").replace("m", "000000"))
        except ValueError:
            return await interaction.response.send_message(embed=create_embed("Error", f"{E_ERROR} Invalid amount entered.", 0xff0000), ephemeral=True)
            
        if amount <= 0:
            return await interaction.response.send_message(embed=create_embed("Error", f"{E_ERROR} Amount must be greater than 0.", 0xff0000), ephemeral=True)

        w = get_wallet(interaction.user.id)
        if w.get("pc", 0) < amount:
            return await interaction.response.send_message(embed=create_embed("Insufficient PC", f"{E_ERROR} You only have **{w.get('pc', 0):,}** {E_PC}.", 0xff0000), ephemeral=True)

        # Calculate Timer
        reduction_hours = 0
        user_roles = [r.id for r in interaction.user.roles]
        for role_id, hours in ROLE_REDUCTIONS_HOURS.items():
            if role_id in user_roles:
                reduction_hours = max(reduction_hours, hours)

        base_wait_hours = 168 # 7 Days
        final_wait_hours = max(0, base_wait_hours - reduction_hours)
        
        now = datetime.now()
        unlocks_at = now + timedelta(hours=final_wait_hours)
        
        claim_id = f"c{get_next_id('pc_claim_id')}"
        
        # Save to DB
        db.pc_claims.insert_one({
            "id": claim_id,
            "user_id": str(interaction.user.id),
            "amount": amount,
            "market_id": self.market_id_input.value,
            "status": "PENDING",
            "created_at": now,
            "unlocks_at": unlocks_at,
            "alert_sent": False
        })
        
        desc = (
            f"{E_GOLD_TICK} **Claim ID:** `{claim_id}`\n"
            f"{E_PC} **Amount:** {amount:,}\n"
            f"{E_ITEMBOX} **Market ID:** {self.market_id_input.value}\n\n"
            f"{E_TIMER} **Approval Time:** <t:{int(unlocks_at.timestamp())}:R>"
        )
        await interaction.response.send_message(embed=create_embed(f"{E_SUCCESS} Withdrawal Submitted", desc, 0x2ecc71), ephemeral=True)

class PCWithdrawView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        
    @discord.ui.button(label="Fill Withdrawal Form", style=discord.ButtonStyle.success, emoji=discord.PartialEmoji.from_str(E_PC))
    async def open_form(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PCWithdrawModal())

async def pc_claim_alert_task():
    """Background loop to alert admins when a user's PC claim timer ends."""
    await bot.wait_until_ready()
    channel = bot.get_channel(PC_APPROVAL_CHANNEL_ID)
    
    while not bot.is_closed():
        if channel:
            now = datetime.now()
            # Find all pending claims where the timer has ended and no alert was sent
            ready_claims = db.pc_claims.find({"status": "PENDING", "alert_sent": False, "unlocks_at": {"$lte": now}})
            
            for claim in ready_claims:
                user = bot.get_user(int(claim['user_id']))
                username = user.name if user else f"Unknown ({claim['user_id']})"
                
                desc = (
                    f"{E_CROWN} **User:** <@{claim['user_id']}> ({username})\n"
                    f"{E_PC} **Amount:** {claim['amount']:,}\n"
                    f"{E_ITEMBOX} **Market ID:** {claim['market_id']}\n"
                    f"{E_TIMER} **Requested:** <t:{int(claim['created_at'].timestamp())}:f>"
                )
                embed = create_embed(f"{E_ALERT} Claim Ready for Approval: {claim['id']}", desc, 0xf1c40f)
                
                await channel.send(content=f"<@&{PC_PING_ROLE_ID}>", embed=embed)
                db.pc_claims.update_one({"_id": claim["_id"]}, {"$set": {"alert_sent": True}})
                
        await asyncio.sleep(60) # Check every 60 seconds

class Paginator(View):
    def __init__(self, ctx, data, title, color, per_page=10):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.data = data
        self.title = title
        self.color = color
        self.per_page = per_page
        self.current_page = 0
        self.total_pages = max(1, (len(data) + per_page - 1) // per_page)
        self.update_buttons()
    def update_buttons(self):
        self.children[0].disabled = self.current_page == 0
        self.children[1].disabled = self.current_page == self.total_pages - 1
    def get_embed(self):
        start = self.current_page * self.per_page
        end = start + self.per_page
        page_data = self.data[start:end]
        embed = discord.Embed(title=f"{self.title} ({self.current_page + 1}/{self.total_pages})", color=self.color)
        for name, value in page_data: embed.add_field(name=name, value=value, inline=False)
        return embed
    @discord.ui.button(emoji="⬅️", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not your menu.", ephemeral=True)
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)
    @discord.ui.button(emoji="➡️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not your menu.", ephemeral=True)
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

# ==============================================================================
#  PHASE 2: DUELIST TRANSFERS & CONTRACTS UI
# ==============================================================================

class TransferBuyView(discord.ui.View):
    def __init__(self, buyer_club, old_club, duelist, price):
        super().__init__(timeout=None)
        self.buyer_club = buyer_club
        self.old_club = old_club
        self.duelist = duelist
        self.price = price

    @discord.ui.button(label="Accept Transfer", style=discord.ButtonStyle.success, custom_id="tb_accept")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 1. Check if buyer still has funds
        buyer_w = get_wallet(self.buyer_club["owner_id"])
        if buyer_w.get("balance", 0) < self.price:
            return await interaction.response.send_message(f"{E_ERROR} The buying club no longer has enough funds to complete this transfer.", ephemeral=True)
            
        # 2. Transfer the Money
        wallets_col.update_one({"user_id": str(self.buyer_club["owner_id"])}, {"$inc": {"balance": -self.price}})
        if self.old_club and self.old_club.get("owner_id"):
            wallets_col.update_one({"user_id": str(self.old_club["owner_id"])}, {"$inc": {"balance": self.price}})
            
        # 3. Transfer the Duelist
        duelists_col.update_one(
            {"_id": self.duelist["_id"]}, 
            {"$set": {"club_id": self.buyer_club["_id"], "transfer_listed": False, "status": "Signed"}}
        )
        # Clear pending transfers
        db.pending_transfers.delete_many({"duelist_id": self.duelist["_id"]})
        
        # Disable buttons
        for child in self.children: child.disabled = True
        await interaction.response.edit_message(embed=create_embed("Transfer Complete", f"{E_SUCCESS} You are now signed to **{self.buyer_club['name']}**!", 0x2ecc71), view=self)
        
        # Notify Owners
        try:
            buyer = await bot.fetch_user(int(self.buyer_club["owner_id"]))
            await buyer.send(embed=create_embed("Transfer Accepted", f"{E_SUCCESS} <@{self.duelist['user_id']}> accepted your transfer offer and has joined **{self.buyer_club['name']}**!", 0x2ecc71))
            if self.old_club and self.old_club.get("owner_id"):
                old_owner = await bot.fetch_user(int(self.old_club["owner_id"]))
                await old_owner.send(embed=create_embed("Transfer Complete", f"{E_MONEY} <@{self.duelist['user_id']}> has been sold to **{self.buyer_club['name']}**. **${self.price:,}** has been added to your balance.", 0x2ecc71))
        except: pass

    @discord.ui.button(label="Put on Hold", style=discord.ButtonStyle.secondary, custom_id="tb_hold")
    async def hold(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Save to pending deals
        db.pending_transfers.insert_one({
            "duelist_id": self.duelist["_id"],
            "buyer_club_id": self.buyer_club["_id"],
            "old_club_id": self.old_club["_id"] if self.old_club else None,
            "price": self.price,
            "status": "HOLD"
        })
        for child in self.children: child.disabled = True
        await interaction.response.edit_message(embed=create_embed("Transfer on Hold", f"{E_TIMER} Deal moved to pending. Use `.pendingtransfer` later to review it.", 0xf1c40f), view=self)

    @discord.ui.button(label="Reject Offer", style=discord.ButtonStyle.danger, custom_id="tb_reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children: child.disabled = True
        await interaction.response.edit_message(embed=create_embed("Transfer Rejected", f"{E_DANGER} You rejected the transfer to **{self.buyer_club['name']}**.", 0xff0000), view=self)
        try:
            buyer = await bot.fetch_user(int(self.buyer_club["owner_id"]))
            await buyer.send(embed=create_embed("Transfer Rejected", f"{E_DANGER} <@{self.duelist['user_id']}> rejected your transfer offer.", 0xff0000))
        except: pass


class ContractSalaryModal(discord.ui.Modal, title="Set Contract Salary"):
    cash_input = discord.ui.TextInput(label="Cash Salary ($)", placeholder="e.g. 500000", style=discord.TextStyle.short)
    pc_input = discord.ui.TextInput(label="PC Salary", placeholder="e.g. 10000", style=discord.TextStyle.short)

    def __init__(self, club, duelist, seasons, role):
        super().__init__()
        self.club = club
        self.duelist = duelist
        self.seasons = seasons
        self.role = role

    async def on_submit(self, interaction: discord.Interaction):
        cash = int(self.cash_input.value.replace(",", "").replace("k", "000").replace("m", "000000")) if self.cash_input.value else 0
        pc = int(self.pc_input.value.replace(",", "").replace("k", "000").replace("m", "000000")) if self.pc_input.value else 0
        
        # Save pending contract
        contract_id = f"cnt_{get_next_id('contract_id')}"
        db.pending_contracts.insert_one({
            "id": contract_id, "club_id": self.club["_id"], "duelist_id": self.duelist["_id"],
            "seasons": self.seasons, "role": self.role, "cash_salary": cash, "pc_salary": pc
        })
        
        await interaction.response.send_message(f"{E_SUCCESS} Contract offer sent to <@{self.duelist['user_id']}>!", ephemeral=True)
        
        # DM the Duelist
        try:
            duelist_user = await bot.fetch_user(int(self.duelist["user_id"]))
            desc = (
                f"{E_CROWN} **Club:** {self.club['name']}\n"
                f"{E_STARS} **Role:** {self.role}\n"
                f"{E_TIMER} **Duration:** {self.seasons} Seasons\n\n"
                f"**Salary:**\n{E_MONEY} **${cash:,}** Cash\n{E_PC} **{pc:,}** PC"
            )
            view = ContractAcceptView(contract_id)
            await duelist_user.send(embed=create_embed(f"{E_BOOK} New Contract Offer", desc, 0xf1c40f), view=view)
        except: pass

class ContractAcceptView(discord.ui.View):
    def __init__(self, contract_id):
        super().__init__(timeout=None)
        self.contract_id = contract_id

    @discord.ui.button(label="Sign Contract", style=discord.ButtonStyle.success)
    async def sign(self, interaction: discord.Interaction, button: discord.ui.Button):
        cnt = db.pending_contracts.find_one({"id": self.contract_id})
        if not cnt: return await interaction.response.send_message("Contract expired or invalid.", ephemeral=True)
        
        # Move to active contracts
        db.contracts.insert_one(cnt)
        db.pending_contracts.delete_many({"duelist_id": cnt["duelist_id"]}) # Delete other offers
        
        for child in self.children: child.disabled = True
        await interaction.response.edit_message(embed=create_embed("Contract Signed", f"{E_SUCCESS} You are officially signed!", 0x2ecc71), view=self)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        db.pending_contracts.delete_one({"id": self.contract_id})
        for child in self.children: child.disabled = True
        await interaction.response.edit_message(embed=create_embed("Contract Declined", f"{E_DANGER} You rejected the contract.", 0xff0000), view=self)

# ==============================================================================
#  CLUB TAX SYSTEM: BACKGROUND TASK & UI
# ==============================================================================

class PayTaxView(discord.ui.View):
    def __init__(self, ctx, club, tax_amount):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.club = club
        self.tax_amount = tax_amount

    @discord.ui.button(label="Pay Tax", style=discord.ButtonStyle.success, custom_id="pay_tax_yes")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.ctx.author.id: return
        
        w = get_wallet(interaction.user.id)
        if w.get("balance", 0) < self.tax_amount:
            return await interaction.response.send_message(embed=create_embed("Insufficient Funds", f"{E_ERROR} You need **${self.tax_amount:,}** {E_MONEY} to pay the tax for **{self.club['name']}**.", 0xff0000), ephemeral=True)
            
        # Deduct cash & update club tax due date (+30 days) and reset warning stages
        wallets_col.update_one({"user_id": str(interaction.user.id)}, {"$inc": {"balance": -self.tax_amount}})
        
        # Add 30 days to the deadline
        current_due = self.club.get("tax_due_date", datetime.now())
        new_due = max(datetime.now(), current_due) + timedelta(days=30)
        
        clubs_col.update_one({"_id": self.club["_id"]}, {"$set": {"tax_due_date": new_due, "tax_reminder_stage": 0}})
        
        # Disable buttons
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        
        # Send Premium DM
        try:
            dm_desc = f"{E_SUCCESS} Your tax of **${self.tax_amount:,}** {E_MONEY} for **{self.club['name']}** has been successfully paid!\n\n{E_TIMER} **Next Payment Due:** <t:{int(new_due.timestamp())}:f>"
            await interaction.user.send(embed=create_embed(f"{E_CROWN} Tax Renewed", dm_desc, 0x2ecc71))
        except: pass
        
        await interaction.followup.send(embed=create_embed("Tax Paid", f"{E_SUCCESS} Tax successfully paid for **{self.club['name']}**.", 0x2ecc71))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="pay_tax_no")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.ctx.author.id: return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        
        try:
            await interaction.user.send(embed=create_embed(f"{E_ERROR} Payment Cancelled", f"You cancelled the tax payment for **{self.club['name']}**.", 0xff0000))
        except: pass
        await interaction.followup.send("Tax payment cancelled.", ephemeral=True)

async def club_tax_alert_task():
    """Background loop to check club taxes, send DMs, and disown expired clubs."""
    await bot.wait_until_ready()
    
    # Reminder stages mapping (Hours Left -> Stage Level)
    stages = [
        (360, 1, "15 Days"), (240, 2, "10 Days"), (120, 3, "5 Days"), 
        (72, 4, "3 Days"), (48, 5, "2 Days"), (24, 6, "1 Day"), 
        (12, 7, "12 Hours"), (6, 8, "6 Hours"), (1, 9, "1 Hour")
    ]
    
    while not bot.is_closed():
        now = datetime.now()
        # Find all owned clubs
        owned_clubs = clubs_col.find({"owner_id": {"$ne": None}})
        
        for club in owned_clubs:
            due_date = club.get("tax_due_date")
            if not due_date: continue 
            
            time_left = due_date - now
            hours_left = time_left.total_seconds() / 3600
            current_stage = club.get("tax_reminder_stage", 0)
            
            # Check for Expiration (Disown Club)
            if hours_left <= 0:
                clubs_col.update_one({"_id": club["_id"]}, {"$set": {"owner_id": None, "tax_due_date": None, "tax_reminder_stage": 0}})
                try:
                    user = bot.get_user(int(club["owner_id"])) or await bot.fetch_user(int(club["owner_id"]))
                    desc = f"{E_DANGER} Your ownership of **{club['name']}** has been officially revoked because you failed to pay the required 25% club tax in time. \n\nThe club is now unsold and back on the public market."
                    await user.send(embed=create_embed(f"{E_ALERT} Club Disowned", desc, 0xff0000))
                except: pass
                continue
            
            # Check Reminders
            for req_hours, stage_num, time_text in stages:
                if hours_left <= req_hours and current_stage < stage_num:
                    tax_amount = int(club.get("value", 0) * 0.25) # 25% of Live Worth
                    try:
                        user = bot.get_user(int(club["owner_id"])) or await bot.fetch_user(int(club["owner_id"]))
                        desc = (
                            f"{E_ALERT} Your club **{club['name']}** has pending taxes!\n\n"
                            f"{E_MONEY} **Tax Amount:** ${tax_amount:,}\n"
                            f"{E_TIMER} **Time Remaining:** {time_text} (<t:{int(due_date.timestamp())}:R>)\n\n"
                            f"Use `.paytax {club['name']}` in the server to pay and avoid losing your club!"
                        )
                        await user.send(embed=create_embed(f"{E_CROWN} Tax Reminder: {time_text} Left", desc, 0xf1c40f))
                    except: pass
                    
                    clubs_col.update_one({"_id": club["_id"]}, {"$set": {"tax_reminder_stage": stage_num}})
                    break # Only trigger one stage per loop cycle

        await asyncio.sleep(600) # Check the database every 10 minutes

@tasks.loop(hours=1) # Runs exactly every 15 seconds!
async def club_market_simulation_task():
    """Background loop to fluctuate club values."""
    try:
        clubs = list(clubs_col.find({})) 
        if not clubs:
            print("Market Loop: No clubs found in database yet.")
            return

        for club in clubs:
            current_value = club.get("value", 1000000)
            
            # Fluctuate between -8% and +10%
            fluctuation_modifier = random.uniform(0.92, 1.10)
            new_value = int(current_value * fluctuation_modifier)
            
            # Hard floor so a club never drops below $100k
            if new_value < 100000:
                new_value = random.randint(100000, 150000)
                
            # Update the database
            clubs_col.update_one(
                {"_id": club["_id"]},
                {"$set": {
                    "value": new_value,
                    "previous_value": current_value 
                }}
            )
        print("Market Loop: Successfully updated club prices!")
            
    except Exception as e:
        print(f"MARKET SIMULATION ERROR: {e}")
        
@club_market_simulation_task.before_loop
async def before_market_loop():
    await bot.wait_until_ready()

#AND THEN IN YOUR on_ready() EVENT, START IT LIKE THIS:
# club_market_simulation_task.start()     
    
class ParticipantView(discord.ui.View):
    def __init__(self, message_id, required_roles=None):
        super().__init__(timeout=None)
        self.message_id = message_id
        self.required_roles = required_roles
    @discord.ui.button(label="Check Participants", style=discord.ButtonStyle.gray, emoji=discord.PartialEmoji.from_str(E_ADMIN))
    async def check_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator: return await interaction.response.send_message(f"{E_ERROR} Admins only.", ephemeral=True)
        message = await interaction.channel.fetch_message(self.message_id)
        reaction = None
        emoji_to_check = "🎉"
        if E_GIVEAWAY.startswith("<"): emoji_to_check = E_GIVEAWAY 
        for r in message.reactions:
             if str(r.emoji) == str(emoji_to_check) or str(r.emoji) == "🎉":
                 reaction = r
                 break
        if not reaction: return await interaction.response.send_message("No entries yet.", ephemeral=True)
        users = [user async for user in reaction.users() if not user.bot]
        valid_participants = []
        for u in users:
            if isinstance(u, discord.Member):
                if self.required_roles:
                    user_role_ids = [r.id for r in u.roles]
                    if isinstance(self.required_roles, list): 
                        if any(rid in self.required_roles for rid in user_role_ids): valid_participants.append(f"• {u.display_name}") 
                    elif isinstance(self.required_roles, int):
                         if self.required_roles in user_role_ids: valid_participants.append(f"• {u.display_name}")
                else: valid_participants.append(f"• {u.display_name}")
        count = len(valid_participants)
        text = "\n".join(valid_participants[:40])
        if count > 40: text += f"\n...and {count-40} more."
        if count == 0: text = "No valid entries found."
        await interaction.response.send_message(f"**Valid Entries:** {count}\n\n{text}", ephemeral=True)

def create_embed(title, description, color=0x2ecc71, thumbnail=None, footer=None, image=None):
    embed = discord.Embed(title=title, description=description, color=color)
    if thumbnail and isinstance(thumbnail, str) and (thumbnail.startswith("http://") or thumbnail.startswith("https://")):
        embed.set_thumbnail(url=thumbnail)
    if image and isinstance(image, str) and (image.startswith("http://") or image.startswith("https://")):
        embed.set_image(url=image)
    if footer: embed.set_footer(text=footer)
    return embed

def resolve_emoji(item):
    if isinstance(item, int) or (isinstance(item, str) and item.isdigit()):
        e = bot.get_emoji(int(item))
        if e: return str(e)
        return f"<:e:{item}>"
    if isinstance(item, str) and item.startswith(":") and item.endswith(":"):
        name = item.strip(":")
        e = discord.utils.get(bot.emojis, name=name)
        if e: return str(e)
        return item
    return str(item)

def log_user_activity(user_id, type, description):
    if db is not None: activities_col.insert_one({"user_id": str(user_id), "type": type, "description": description, "timestamp": datetime.now()})

def log_past_entity(user_id, type, name):
    if db is not None: 
        past_entities_col.insert_one({
            "user_id": str(user_id), 
            "type": type, 
            "name": name, 
            "timestamp": datetime.now()
        })

async def send_log(channel_key, embed):
    if db is None: return
    cid = LOG_CHANNELS.get(channel_key)
    if cid:
        ch = bot.get_channel(cid)
        if ch: await ch.send(embed=embed)

def parse_duration(time_str):
    time_str = time_str.lower()
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    try:
        unit = time_str[-1]
        val = int(time_str[:-1])
        return val * units.get(unit, 60)
    except: return 60

def parse_prize_amount(prize_str):
    try:
        clean = prize_str.lower().replace(",", "").replace("$", "")
        if "k" in clean: return int(float(clean.replace("k", "")) * 1000)
        if "m" in clean: return int(float(clean.replace("m", "")) * 1000000)
        if clean.isdigit(): return int(clean)
    except: return 0
    return 0

# ---------- TASKS & EVENTS ----------
async def market_simulation_task():
    await bot.wait_until_ready()
    print("[Market] Simulation task started.")
    while not bot.is_closed():
        try:
            # Wait 1 hour (3600 seconds)
            await asyncio.sleep(3600)
            
            if db is not None:
                updated_count = 0
                for c in clubs_col.find():
                    # Get current value, fallback to base_price if missing
                    current_val = c.get("value", c.get("base_price", 0))
                    
                    # Fluctuate between -3% and +3%
                    percent_change = random.uniform(-0.03, 0.03)
                    change_amount = int(current_val * percent_change)
                    new_value = max(100, current_val + change_amount) # Minimum value 100
                    
                    clubs_col.update_one({"_id": c["_id"]}, {"$set": {"value": new_value}})
                    updated_count += 1
                
                print(f"[Market] Auto-Updated values for {updated_count} clubs.")
                
                # Optional: Log to Discord Channel
                log_ch = bot.get_channel(LOG_CHANNELS["club"])
                if log_ch: 
                    await log_ch.send(embed=create_embed(f"{E_STARS} Market Update", f"Values for **{updated_count}** clubs have shifted due to market volatility.", 0x3498db))
                    
        except Exception as e:
            print(f"[Market Error] {e}")
            await asyncio.sleep(60) # Wait 1 min before retrying if error

@bot.event
async def on_command_completion(ctx):
    log_user_activity(ctx.author.id, "Command", f"Used {E_CHAT} `.{ctx.command.name}`")

# ==============================================================================
#  GIVEAWAY RECOVERY SYSTEM (Fixes Restart Issue)
# ==============================================================================

async def restart_giveaway_timer(mid, ch, prize, time_left):
    """Helper to restart a timer for a specific giveaway."""
    await asyncio.sleep(time_left)
    await end_giveaway(mid, ch, prize)

async def check_active_giveaways():
    """Checks DB for active giveaways on startup and resumes them."""
    await bot.wait_until_ready()
    
    if db is None: return
    
    # Find all giveaways that haven't been marked as ended
    active_gws = giveaways_col.find({"ended": False})
    
    count = 0
    for gw in active_gws:
        try:
            channel = bot.get_channel(gw['channel_id'])
            if not channel: continue # Channel deleted?
            
            # Calculate remaining time
            now = datetime.now().timestamp()
            end_time = gw['end_time']
            remaining = end_time - now
            
            if remaining <= 0:
                # Giveaway ended while bot was offline -> Finish it NOW
                print(f"[Giveaway] Ending expired giveaway {gw['message_id']}")
                await end_giveaway(gw['message_id'], channel, gw['prize'])
            else:
                # Giveaway still running -> Restart the timer
                print(f"[Giveaway] Resuming giveaway {gw['message_id']} ({int(remaining)}s left)")
                bot.loop.create_task(restart_giveaway_timer(gw['message_id'], channel, gw['prize'], remaining))
            
            count += 1
        except Exception as e:
            print(f"[Giveaway Error] Failed to resume {gw.get('message_id')}: {e}")
            
    if count > 0:
        print(f"[System] Resumed/Ended {count} active giveaways.")

@bot.event
async def on_message(message):
    # 1. Ignore bots
    if message.author.bot: 
        return
        
    # 2. Crash-proof channel check using .get()
    if db is not None and message.channel.id == LOG_CHANNELS.get("chat_channel"):
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # --- PC BOX SYSTEM ---
        ret = message_counts_col.find_one_and_update(
            {"user_id": str(message.author.id), "date": today_str},
            {"$inc": {"count": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        
        if ret and ret.get("count", 0) % 150 == 0:
            wallets_col.update_one({"user_id": str(message.author.id)}, {"$inc": {"pc_boxes": 1}}, upsert=True)
            try:
                desc = f"You just sent 150 messages today and earned **1x PC Box**!\nType `.ob` to open it."
                await message.author.send(embed=create_embed(f"{E_ITEMBOX} Box Earned!", desc, 0x2ecc71))
            except: 
                pass
        
        await update_quest(message.author.id, "msgs", 1)
        
        # --- LEVEL UP SYSTEM ---
        w_ret = wallets_col.find_one_and_update(
            {"user_id": str(message.author.id)},
            {"$inc": {"lifetime_msgs": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        new_total = w_ret.get("lifetime_msgs", 1)
        old_total = new_total - 1
        
        old_lvl, _, _ = calc_level_data(old_total)
        new_lvl, _, _ = calc_level_data(new_total)
        
        if new_lvl > old_lvl:
            reward = LEVEL_REWARDS.get(new_lvl)
            reward_txt = ""
            if reward:
                wallets_col.update_one(
                    {"user_id": str(message.author.id)}, 
                    {"$inc": {"pc": reward["pc"], "balance": reward["cash"]}}
                )
                reward_txt = f"\n\n{E_GIVEAWAY} **Rewards Unlocked:**\n{E_PC} **{reward['pc']:,} PC**\n{E_MONEY} **${reward['cash']:,} Cash**"
            
            try:
                desc = f"You reached **Level {new_lvl}** in the main chat!{reward_txt}"
                await message.author.send(embed=create_embed(f"{E_STARS} Level Up!", desc, 0xf1c40f))
            except: 
                pass

    # 3. CRITICAL: This line tells the bot to actually read your commands!
    # Notice how it is outdented all the way to the left so it runs no matter what.
    await bot.process_commands(message)

@bot.listen('on_message')
async def auto_deposit_listener(message):
    # Ignore messages outside the market channel
    if message.channel.id != 1483437925139218613:
        return

    # ==========================================================
    # 1. LISTING THE POKEMON (Strictly PokéTwo Only)
    # ==========================================================
    if message.author.id == 716390085896962058: 
        if "Listed your" in message.content and "on the market for" in message.content:
            print("[MARKET DEBUG] Detected a new market listing from PokéTwo!")
            
            match_amount = re.search(r"for \*\*([\d,]+)\*\* Pokécoins", message.content)
            match_id = re.search(r"\(Listing #(\d+)\)", message.content)
            
            if match_amount and match_id:
                amount = int(match_amount.group(1).replace(",", ""))
                market_id = match_id.group(1)
                print(f"[MARKET DEBUG] Success! Amount: {amount} | Market ID: {market_id}")

                deposit = deposits_col.find_one({"status": "Queued", "amount": amount}, sort=[("created_at", 1)])
                if deposit:
                    deposits_col.update_one(
                        {"_id": deposit["_id"]},
                        {"$set": {"status": "On Hold", "market_id": market_id, "listed_at": datetime.now(timezone.utc)}}
                    )
                    
                    user = bot.get_user(int(deposit["user_id"]))
                    if user:
                        td = datetime.now(timezone.utc) - deposit["created_at"].replace(tzinfo=timezone.utc)
                        time_taken = f"{int(td.total_seconds())} seconds"
                        desc = (
                            f"Your deposit request `#{deposit['deposit_id']}` is ready.\n\n"
                            f"**Amount:** {amount:,} PC\n"
                            f"**Market ID:** `{market_id}`\n"
                            f"**Bot processing time:** {time_taken}\n\n"
                            f"⚠️ **Please buy this exact listing on the market to complete your deposit.** Do not buy any other listing."
                        )
                        try:
                            await user.send(embed=create_embed("Market ID Ready", desc, 0x3498db))
                            print("[MARKET DEBUG] Sent DM to user successfully.")
                        except discord.Forbidden:
                            print("[MARKET DEBUG] ERROR: User has DMs closed.")
                else:
                    print("[MARKET DEBUG] Could not find a Queued deposit matching this amount.")
            else:
                print("[MARKET DEBUG] ERROR: Regex failed to extract amount or ID.")


    # ==========================================================
    # 2. CONFIRMING THE PURCHASE (Listens to ANYONE!)
    # ==========================================================
    if "Someone purchased your" in message.content and "You received" in message.content:
        # Check if Ze Bot is pinged in the message (either by Poketwo or a User)
        if str(bot.user.id) in message.content or bot.user in message.mentions:
            print(f"[MARKET DEBUG] Detected a purchase confirmation from {message.author.name}!")
            
            match = re.search(r"You received ([\d,]+) Pokécoins", message.content)
            if match:
                amount = int(match.group(1).replace(",", ""))
                print(f"[MARKET DEBUG] Success! Sold for: {amount}")

                deposit = deposits_col.find_one({"status": "On Hold", "amount": amount}, sort=[("listed_at", 1)])
                if deposit:
                    print(f"[MARKET DEBUG] Found matching On Hold deposit: {deposit['deposit_id']}")
                    deposits_col.update_one(
                        {"_id": deposit["_id"]},
                        {"$set": {"status": "Completed"}}
                    )
                    
                    wallets_col.update_one({"user_id": deposit["user_id"]}, {"$inc": {"pc": amount}}, upsert=True)
                    user = bot.get_user(int(deposit["user_id"]))
                    
                    if user:
                        try:
                            dm_desc = f"{E_SUCCESS} Your deposit of **{amount:,} PC** (ID: `{deposit['deposit_id']}`) is fully confirmed!\n💰 The PC has been added to your bot account."
                            await user.send(embed=create_embed("Deposit Confirmed", dm_desc, 0x2ecc71))
                        except:
                            pass

                    # Log it! Now tracks WHO confirmed it.
                    log_channel = bot.get_channel(1483526389339521066)
                    if log_channel:
                        log_desc = (
                            f"**Deposit ID:** `{deposit['deposit_id']}`\n"
                            f"**User:** <@{deposit['user_id']}>\n"
                            f"**Amount:** {amount:,} PC\n"
                            f"**Status:** {E_SUCCESS} Successfully added PC\n"
                            f"**Confirmed By:** {message.author.mention}"
                        )
                        await log_channel.send(embed=discord.Embed(title="Deposit Log: Completed", description=log_desc, color=0x2ecc71))
                        print("[MARKET DEBUG] Log sent to admin channel. Process complete!")
                else:
                    print("[MARKET DEBUG] ERROR: Could not find an 'On Hold' deposit for this amount.")
            else:
                print("[MARKET DEBUG] ERROR: Regex failed to read the sold amount.")

# ==========================================================
# 🛑 ECONOMY KILL SWITCH SHIELD
# ==========================================================
@bot.check
async def economy_lockdown_shield(ctx):
    # If the switch is ON (default), let everything run normally
    if getattr(bot, 'p2_economy_open', True):
        return True
        
    # The master list of all PC/Economy commands and their aliases
    locked_commands = [
        "wallet", "bal", "balance",
        "boxes", "box", "massbox", "mbox",
        "profile", "p", "pr", "i", "I", "P",
        "shop", "buy",
        "depositpc", "dpc", "depositpcstatus", "dpcs", "pendingdeposits", "pdpc", "logdepositpc",
        "withdrawpc", "withdraw", "wpc",
        "lr", "loginreward", "ob" , "getpc", "cs", "claimstatus",
    ]
    
    # If the user tries to use ANY of those commands while the system is stopped...
    if ctx.command:
        if ctx.command.name in locked_commands or any(alias in locked_commands for alias in ctx.command.aliases):
            # Silently cancel the command with ZERO response to the user
            raise commands.CheckFailure("SilentEconomyLockdown")
            
    # If it's a club, duelist, or giveaway command, let it pass through!
    return True
                
async def check_login_reminders():
    """Checks for expired login cooldowns and pings users."""
    await bot.wait_until_ready()
    
    # FIX: Use the variable directly, do not use ["login_log"]
    channel = bot.get_channel(LOGIN_LOG_CHANNEL_ID)
    
    if not channel:
        print(f"[Warning] Login Log Channel (ID: {LOGIN_LOG_CHANNEL_ID}) not found.")
    
    while not bot.is_closed():
        if channel:
            now = datetime.now()
            # Find users who:
            # 1. Have reminders enabled
            # 2. Have NOT been reminded yet for this cycle
            # 3. Have a last_login date recorded
            users = wallets_col.find({
                "remind_login": True,
                "reminder_sent": False, # This flag resets when they use .login
                "last_login": {"$ne": None}
            })

            for user in users:
                try:
                    last_login = user["last_login"]
                    # Ensure last_login is datetime
                    if not isinstance(last_login, datetime): continue
                    
                    next_claim = last_login + timedelta(hours=24)
                    
                    # Check if time has passed
                    if now >= next_claim:
                        # OFFINE CATCH-UP LOGIC:
                        # Only remind if the deadline passed within the last 12 hours.
                        time_diff = now - next_claim
                        if time_diff < timedelta(hours=12):
                            # Construct Premium Embed
                            embed = discord.Embed(
                                title=f"{E_TIMER} Login Ready!",
                                description=f"Your 24-hour cooldown has ended.\nUse `/login` now to keep your streak alive!",
                                color=0x3498db
                            )
                            embed.add_field(name=f"{E_BOOST} Current Streak", value=f"**{user.get('login_streak', 0)} Days**", inline=True)
                            embed.set_footer(text="Disable this via /remindlogin")
                            if bot.user.avatar: embed.set_thumbnail(url=bot.user.avatar.url)

                            # Send Ping + Embed
                            await channel.send(content=f"<@{user['user_id']}>", embed=embed)

                        # Mark as sent so we don't spam
                        wallets_col.update_one({"_id": user["_id"]}, {"$set": {"reminder_sent": True}})
                except Exception as e:
                    print(f"[Reminder Loop Error] {e}")

        await asyncio.sleep(60) # Check every minute

# ==============================================================================
#  DUELIST SYSTEM: CORE & EVENTS
# ==============================================================================

def get_duelist(identifier):
    """Fetches a duelist by Discord ID mention, or Duelist ID."""
    if identifier.startswith("<@") and identifier.endswith(">"):
        user_id = identifier.replace("<@", "").replace("!", "").replace(">", "")
        return duelists_col.find_one({"user_id": user_id})
    else:
        return duelists_col.find_one({"duelist_id": identifier.upper()})

@bot.event
async def on_member_remove(member):
    # Auto-handle duelists leaving the server
    if db is None: return
    duelist = duelists_col.find_one({"user_id": str(member.id)})
    
    if duelist:
        club_id = duelist.get("club_id")
        if club_id:
            # Refund the club owner or group fund
            club = clubs_col.find_one({"_id": club_id})
            if club:
                refund = duelist.get("last_purchase_price", 0)
                owner_id = club.get("owner_id")
                if owner_id:
                    wallets_col.update_one({"user_id": owner_id}, {"$inc": {"balance": refund}})
                
        # Update duelist status
        duelists_col.update_one(
            {"_id": duelist["_id"]}, 
            {"$set": {"status": "Left the Server", "club_id": None, "transfer_listed": False}}
        )

def get_group_total_shares(group_name):
    """Calculates total shares owned in a group."""
    if db is None: return 0
    members = list(group_members_col.find({"group_name": group_name.lower()}))
    return sum(m.get("share_percentage", 0) for m in members)

# ===========================
#   GROUP 1: ECONOMY & PROFILE
# ===========================

@bot.command(name="playerhistory", aliases=["ph"], description="Admin: View full user history.")
@commands.has_permissions(administrator=True)
async def playerhistory(ctx, user: discord.Member):
    uid = str(user.id)
    w = wallets_col.find_one({"user_id": uid})
    bal = w.get("balance", 0) if w else 0
    past_clubs = list(past_entities_col.find({"user_id": uid, "type": "ex_owner"}))
    past_groups = list(past_entities_col.find({"user_id": uid, "type": "ex_member"}))
    acts = list(activities_col.find({"user_id": uid}).sort("timestamp", -1).limit(50))
    data = []
    summary = f"**Wallet:** ${bal:,}\n**Ex-Clubs:** {', '.join([p['name'] for p in past_clubs]) or 'None'}\n**Ex-Groups:** {', '.join([p['name'].title() for p in past_groups]) or 'None'}"
    data.append((f"{E_CROWN} User Summary", summary))
    for act in acts:
        ts = act['timestamp'].strftime("%Y-%m-%d %H:%M")
        icon = E_CHAT if act['type'] == "Command" else E_MONEY
        data.append((f"{icon} {act['type']} - {ts}", f"{act['description']}"))
    if not data: data.append(("No History", "This user has no recorded history."))
    view = Paginator(ctx, data, f"{E_BOOK} History: {user.display_name}", 0x3498db, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="profile", aliases=["pr","p","i","I","P"], description="View profile stats and currencies.")
async def profile(ctx, member: discord.Member = None):
    member = member or ctx.author
    uid = str(member.id)
    w = wallets_col.find_one({"user_id": uid})
    cash = w.get("balance", 0) if w else 0
    pc = w.get("pc", 0) if w else 0
    shiny = w.get("shiny_coins", 0) if w else 0
    thumbnail_url = member.avatar.url if member.avatar else None
    group_mem = group_members_col.find_one({"user_id": uid})
    if group_mem:
        g_info = groups_col.find_one({"name": group_mem['group_name']})
        if g_info and g_info.get('logo'): thumbnail_url = g_info['logo']
    embed = create_embed(f"{E_CROWN} User Profile", f"**User:** {member.mention}", 0x3498db, thumbnail=thumbnail_url)
    embed.add_field(name="Wallet", value=(f"{E_MONEY} Cash: **${cash:,}**\n{E_PC} PC: **{pc:,}**\n{E_SHINY} Shiny Coins: **{shiny:,}**"), inline=False)
    groups = list(group_members_col.find({"user_id": uid}))
    g_list = [f"{g['group_name'].title()} ({g['share_percentage']}%)" for g in groups]
    embed.add_field(name="Groups", value=", ".join(g_list) if g_list else "None", inline=False)
    prof = profiles_col.find_one({"user_id": uid})
    if prof and prof.get("owned_club_id"):
        c = clubs_col.find_one({"id": prof["owned_club_id"]})
        if c: embed.add_field(name="Owned Club", value=f"{c['name']} (100%)", inline=False)
           
    # --- INDIVIDUAL AWARDS INJECTION ---
    awards_text = ""
    if w and w.get("t_ballondor", 0) > 0:
        b = w["t_ballondor"]
        awards_text += f"**Ballon d'Ors**\n{b}x " + (E_BALLONDOR * b) + "\n\n"
    if w and w.get("t_sballondor", 0) > 0:
        sb = w["t_sballondor"]
        awards_text += f"**Super Ballon d'Ors**\n{sb}x " + (E_SUPERBALLONDOR * sb) + "\n\n"
        
    if awards_text:
        embed.add_field(name=f"{E_STARS} Player Awards", value=awards_text.strip(), inline=False)
    # -----------------------------------
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="wallet", aliases=["wl","balance", "bal", "Bal"], description="Check your balance.")
async def wallet(ctx):
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    cash = w.get("balance", 0) if w else 0
    pc = w.get("pc", 0) if w else 0
    shiny = w.get("shiny_coins", 0) if w else 0
    embed = create_embed(f"{E_MONEY} Wallet Balance", f"**User:** {ctx.author.mention}\n\n{E_MONEY} **Cash:** ${cash:,}\n{E_PC} **PC:** {pc:,}\n{E_SHINY} **Shiny Coins:** {shiny:,}", 0x2ecc71, thumbnail=ctx.author.avatar.url if ctx.author.avatar else None)
    await ctx.send(embed=embed)

# ==============================================================================
#  CHAT LEVELING COMMANDS
# ==============================================================================

@bot.command(name="lvlclaims", aliases=["leveluprewards", "lr"], description="View all chat Level Up rewards.")
async def lvlclaims(ctx):
    desc = f"{E_CHAT} Grind messages in the main chat to level up and earn these milestones!\n\n"
    for lvl, rw in LEVEL_REWARDS.items():
        desc += f"**Level {lvl}:** {rw['pc']:,} {E_PC} | ${rw['cash']:,} {E_MONEY}\n"
        
    embed = create_embed(f"{E_CROWN} Level Up Rewards", desc, 0x3498db)
    await ctx.send(embed=embed)

@bot.command(name="rank", aliases=["level", "lvl"], description="Check a user's chat rank and level.")
async def rank(ctx, member: discord.Member = None):
    target = member or ctx.author
    w = get_wallet(target.id)
    total_msgs = w.get("lifetime_msgs", 0)
    
    level, current_prog, required = calc_level_data(total_msgs)
    
    # Progress Bar
    pct = min(1.0, current_prog / required) if required > 0 else 0
    bar_len = 10
    fill = int(pct * bar_len)
    bar = "🟦" * fill + "⬜" * (bar_len - fill)
    
    desc = (
        f"{E_CROWN} **Current Level:** {level}\n"
        f"{E_CHAT} **Total Messages:** {total_msgs:,}\n\n"
        f"**Progress to Level {level + 1}:**\n"
        f"{bar} `{current_prog:,} / {required:,}`"
    )
    
    embed = create_embed(f"{E_STARS} Chat Rank: {target.display_name}", desc, 0xf1c40f)
    if target.avatar:
        embed.set_thumbnail(url=target.avatar.url)
    await ctx.send(embed=embed)

class LevelLBSelect(discord.ui.Select):
    def __init__(self, ctx):
        options = [
            discord.SelectOption(label="Top 10", emoji="⭐", value="10"),
            discord.SelectOption(label="Top 50", emoji="🌟", value="50"),
            discord.SelectOption(label="Top 100", emoji="👑", value="100")
        ]
        super().__init__(placeholder="Select Leaderboard Size...", min_values=1, max_values=1, options=options)
        self.ctx = ctx
        
    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.ctx.author.id: return
        
        limit = int(self.values[0])
        # Fetch top users sorted by lifetime_msgs
        top_users = list(wallets_col.find({"lifetime_msgs": {"$gt": 0}}).sort("lifetime_msgs", -1).limit(limit))
        
        if not top_users:
            return await interaction.response.send_message("No chat data found yet.", ephemeral=True)
            
        data = []
        for i, u in enumerate(top_users):
            lvl, _, _ = calc_level_data(u.get("lifetime_msgs", 0))
            data.append((f"#{i+1} • <@{u['user_id']}>", f"{E_BOOST} Level: **{lvl}** | {E_CHAT} Msgs: **{u.get('lifetime_msgs', 0):,}**"))
            
        # Use existing paginator for smooth scrolling through 50 or 100 users
        view = Paginator(self.ctx, data, f"{E_CROWN} Top {limit} Active Chatters", 0xf1c40f)
        await interaction.response.edit_message(embed=view.get_embed(), view=view)

class LevelLBView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=60)
        self.add_item(LevelLBSelect(ctx))

@bot.command(name="lvllb", aliases=["levelupleaderboard", "llb"], description="View the Chat Level leaderboard.")
async def lvllb(ctx):
    desc = f"Use the dropdown below to select the Leaderboard size.\n{E_CHAT} Start chatting to climb the ranks!"
    await ctx.send(embed=create_embed(f"{E_STARS} Chat Leaderboard", desc, 0x3498db), view=LevelLBView(ctx))

@bot.hybrid_command(name="buyshiny", aliases=["exchange", "bs"], description="Buy Shiny Coins ($100 Cash = 1 Shiny).")
async def buyshiny(ctx, amount: int):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Amount must be positive.", 0xff0000))
    cost = amount * 100
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    balance = w.get("balance", 0) if w else 0
    if balance < cost:
        return await ctx.send(embed=create_embed("Insufficient Funds", f"You need **${cost:,}** Cash to buy **{amount:,}** Shiny Coins.", 0xff0000))
    wallets_col.update_one(
        {"user_id": str(ctx.author.id)},
        {"$inc": {"balance": -cost, "shiny_coins": amount}}
    )
    log_user_activity(ctx.author.id, "Exchange", f"Bought {amount:,} Shiny Coins for ${cost:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Exchange Successful", f"You paid **${cost:,}** {E_MONEY}\nYou received **{amount:,}** {E_SHINY}", 0x2ecc71))

@bot.hybrid_command(name="buycoins", aliases=["bpc"], description="Convert Cash to Shiny Coins ($100 = 1 Shiny).")
async def buycoins(ctx, amount: int):
    # This is essentially an alias for buyshiny but requested as a specific new feature
    await buyshiny(ctx, amount)

@bot.hybrid_command(name="payout", aliases=["po"], description="Admin: Pay user (System).")
@commands.has_permissions(administrator=True)
async def payout(ctx, user: discord.Member, amount: HumanInt, *, reason: str):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(user.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(user.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(user.id, "Payout", f"Cashed out ${amount:,} by {ctx.author.name}. Reason: {reason}")
    embed_log = create_embed(f"{E_MONEY} Payout Log", f"**Paid To:** {user.mention}\n**Paid By:** {ctx.author.mention}\n**Amount:** ${amount:,}\n**Reason:** {reason}", 0xe74c3c)
    await send_log("withdraw", embed_log)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Payout Successful", f"Processed payout of **${amount:,}** for {user.mention}.", 0x2ecc71))

@bot.hybrid_command(name="withdrawwallet", aliases=["ww"], description="Burn money from wallet.")
async def withdrawwallet(ctx, amount: HumanInt):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Burned ${amount:,} from wallet.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdrawn", f"Removed **${amount:,}** from wallet.", 0x2ecc71))

@bot.hybrid_command(name="daily", aliases=["claim"], description="Claim Daily Reward (100 msgs req).")
async def daily(ctx):
    today = datetime.now().strftime("%Y-%m-%d")
    data = message_counts_col.find_one({"user_id": str(ctx.author.id), "date": today})
    count = data.get("count", 0) if data else 0
    
    if count < DAILY_MSG_REQ: 
        return await ctx.send(embed=create_embed("Daily Locked", f"{E_DANGER} You need **{DAILY_MSG_REQ}** messages today.\nCurrent: **{count}**", 0xff0000))
    
    user = wallets_col.find_one({"user_id": str(ctx.author.id)})
    last = user.get("last_daily") if user else None
    
    if last and (datetime.now() - last) < timedelta(hours=24): 
        next_claim = int((last + timedelta(hours=24)).timestamp())
        return await ctx.send(embed=create_embed("Cooldown", f"Next claim: <t:{next_claim}:R>", 0x95a5a6))
    
    wallets_col.update_one(
        {"user_id": str(ctx.author.id)}, 
        {"$inc": {"balance": 150000, "shiny_coins": 50}, "$set": {"last_daily": datetime.now()}}, 
        upsert=True
    )
    
    await update_quest(ctx.author.id, "daily_cmd", 1)
    
    await ctx.send(embed=create_embed(f"{E_GIVEAWAY} Daily Claimed", f"You received:\n+$10,000 {E_MONEY}\n+5 {E_SHINY}", 0x2ecc71))

# ==============================================================================
#  LOGIN & STREAK SYSTEM
# ==============================================================================

@bot.hybrid_command(name="login", description="Claim daily login rewards & build streaks (24h Cooldown).")
async def login(ctx):
    uid = str(ctx.author.id)
    now = datetime.now()
    
    # 1. Fetch User Data
    user_data = wallets_col.find_one({"user_id": uid})
    
    # Initialize if new user
    if not user_data:
        user_data = {"user_id": uid, "balance": 0, "shiny_coins": 0, "login_streak": 0, "last_login": None}
        wallets_col.insert_one(user_data)
        
    last_login = user_data.get("last_login")
    current_streak = user_data.get("login_streak", 0)
    
    # 2. Check Cooldown (24 Hours)
    if last_login:
        # Ensure last_login is a datetime object (handle legacy/fresh db)
        if not isinstance(last_login, datetime):
            # If it's stored differently or None, assume ready (or reset)
            pass 
        else:
            diff = now - last_login
            if diff < timedelta(hours=24):
                # Calculate future timestamp for Discord relative time tag
                next_claim = int((last_login + timedelta(hours=24)).timestamp())
                return await ctx.send(embed=create_embed(f"{E_ALERT} Cooldown", f"You have already logged in today.\nNext reward available <t:{next_claim}:R>!", 0xff0000))
            
            # Check Streak Validity (48 hours grace period allowed)
            if diff > timedelta(hours=48):
                current_streak = 0 # Streak broken if > 48h since last login
    
    # 3. Calculate Rewards
    current_streak += 1 # Increment for today
    
    base_cash = 100000
    base_sc = 50
    # Streak Bonus: Day 1=0, Day 2=10k, Day 3=20k...
    streak_bonus = (current_streak - 1) * 10000
    
    total_cash = base_cash + streak_bonus
    
   # 4. Update Database
    wallets_col.update_one(
        {"user_id": uid},
        {
            "$inc": {"balance": total_cash, "shiny_coins": base_sc},
            "$set": {
                "last_login": now, 
                "login_streak": current_streak,
                "reminder_sent": False # <--- CRITICAL: Resets the reminder for next time
            }
        },
        upsert=True
    )
    
    await update_quest(ctx.author.id, "login", 1)           # Daily Task
    await update_quest(ctx.author.id, "login_days", 1)      # Weekly/Monthly/Yearly
    await update_quest(ctx.author.id, "login_streak", 1)    # Update progress (logic handles accumulation)
    
    # 5. Build Premium Embed
    desc = (
        f"Welcome back, **{ctx.author.name}**! Here are your rewards:\n\n"
        f"{E_MONEY} **Base Cash:** ${base_cash:,}\n"
        f"{E_BOOST} **Streak Bonus:** ${streak_bonus:,}\n"
        f"{E_SHINY} **Shiny Coins:** {base_sc:,}\n"
        f"──────────────────\n"
        f"{E_SUCCESS} **Total Received:** ${total_cash:,} + {base_sc} SC"
    )
    
    embed = create_embed(f"{E_FIRE} Daily Login (Day {current_streak})", desc, 0x2ecc71)
    if ctx.author.avatar:
        embed.set_thumbnail(url=ctx.author.avatar.url)
    
    # Milestone Flair
    if current_streak % 7 == 0:
        embed.set_footer(text=f"🔥 {current_streak} Day Streak! Amazing dedication!")
    else:
        embed.set_footer(text="Login again in 24h to keep the streak!")
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="grouplist", aliases=["gl"], description="List all investor groups.")
async def grouplist(ctx):
    groups = list(groups_col.find())
    data = []
    for g in groups: data.append((g['name'].title(), f"{E_MONEY} ${g['funds']:,}"))
    view = Paginator(ctx, data, f"{E_PREMIUM} Group List", 0x9b59b6, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="groupinfo", aliases=["gi"], description="Get detailed info about a group.")
async def groupinfo(ctx, *, group_name: str):
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Group not found.", 0xff0000))
    members = list(group_members_col.find({"group_name": gname}))
    clubs = list(clubs_col.find({"owner_id": f"group:{gname}"}))
    embed = discord.Embed(title=f"{E_PREMIUM} Group: {g['name'].title()}", color=0x9b59b6)
    if g.get('logo'): embed.set_thumbnail(url=g['logo'])
    embed.add_field(name="Bank", value=f"{E_MONEY} ${g['funds']:,}", inline=True)
    mlist = []
    for m in members[:15]:
        try: u = await bot.fetch_user(int(m['user_id'])); name = u.name
        except: name = "Unknown"
        mlist.append(f"{E_ARROW} {name}: {m['share_percentage']}%")
    if len(members) > 15: mlist.append(f"...and {len(members)-15} more.")
    embed.add_field(name=f"Members ({len(members)})", value="\n".join(mlist) or "None", inline=False)
    clist = [c['name'] for c in clubs]
    embed.add_field(name="Clubs Owned", value=", ".join(clist) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="creategroup", description="Create a new investment group.")
async def creategroup(ctx, name: str, share: int):
    gname = name.lower()
    
    # 1. Check if group exists
    if groups_col.find_one({"name": gname}): 
        return await ctx.send(embed=create_embed("Error", f"Group **{name}** already exists.", 0xff0000))
    
    # 2. Logic Check: Can't start with > 100%
    if share < 1 or share > 100:
        return await ctx.send(embed=create_embed("Error", "Share must be between 1% and 100%.", 0xff0000))

    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    
    # 3. Create
    groups_col.insert_one({"name": gname, "funds": 0, "owner_id": str(ctx.author.id), "logo": logo_url})
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    
    log_user_activity(ctx.author.id, "Group", f"Created group {name}.")
    
    remaining = 100 - share
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Created", f"Group **{name}** created.\nYou own **{share}%**.\n**{remaining}%** shares available for others.", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="joingroup", description="Join an existing group.")
async def joingroup(ctx, name: str, share: int):
    gname = name.lower()
    
    # 1. Check if group exists
    if not groups_col.find_one({"name": gname}): 
        return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    
    # 2. Check if already member
    if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): 
        return await ctx.send(embed=create_embed("Error", "You are already a member.", 0xff0000))
    
    # 3. CRITICAL FIX: Check Total Shares
    current_total = get_group_total_shares(gname)
    available = 100 - current_total
    
    if share > available:
        return await ctx.send(embed=create_embed(f"{E_DANGER} Share Limit Reached", f"This group only has **{available}%** shares available.\nYou asked for **{share}%**.", 0xff0000))
    
    if share <= 0:
        return await ctx.send(embed=create_embed("Error", "Share must be positive.", 0xff0000))

    # 4. Join
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Joined group {name}.")
    
    new_available = available - share
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Joined", f"You joined **{name}** with **{share}%** equity.\n**{new_available}%** shares remaining.", 0x2ecc71))

@bot.hybrid_command(name="deposit", aliases=["dep"], description="Transfer funds to group.")
async def deposit(ctx, group_name: str, amount: HumanInt):
    if amount <= 0: return
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Not a member.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    groups_col.update_one({"name": gname}, {"$inc": {"funds": amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Deposited ${amount:,} to {group_name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deposit", f"Deposited **${amount:,}** to **{group_name}**.", 0x2ecc71))

@bot.hybrid_command(name="withdraw", aliases=["wd"], description="Withdraw funds from group.")
async def withdraw(ctx, group_name: str, amount: HumanInt):
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    g = groups_col.find_one({"name": gname})
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Withdrew ${amount:,} from {group_name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdraw", f"Withdrew **${amount:,}**.", 0x2ecc71))

@bot.hybrid_command(name="leavegroup", aliases=["lg"], description="Leave a group.")
async def leavegroup(ctx, name: str):
    gname = name.lower()
    mem = group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)})
    if not mem: return await ctx.send(embed=create_embed("Error", "Not a member.", 0xff0000))
    if mem['share_percentage'] > 0: return await ctx.send(embed=create_embed("Error", "Sell shares first.", 0xff0000))
    g = groups_col.find_one({"name": gname})
    penalty = int(g["funds"] * (LEAVE_PENALTY_PERCENT / 100))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -penalty}})
    group_members_col.delete_one({"_id": mem["_id"]})
    past_entities_col.insert_one({"user_id": str(ctx.author.id), "type": "ex_member", "name": gname, "timestamp": datetime.now()})
    log_user_activity(ctx.author.id, "Group", f"Left group {name}.")
    await ctx.send(embed=create_embed(f"{E_DANGER} Left Group", f"Left **{name}**. Penalty: **${penalty:,}**.", 0xff0000))

# ==============================================================================
#  QUEST SYSTEM
# ==============================================================================

# Quest Configuration
QUEST_CONFIG = {
    "daily": {
        "duration": 86400, # 24 Hours
        "bonus": 250000,
        "tasks": {
            "daily_cmd": {"target": 1, "desc": "Claim .daily", "reward": 150000},
            "login": {"target": 1, "desc": "Login once", "reward": 10000},
            "trade": {"target": 1, "desc": "Trade with a user", "reward": 25000},
            "event": {"target": 1, "desc": "Participate in Event", "reward": 30000},
            "giveaway": {"target": 1, "desc": "Enter a Giveaway", "reward": 15000},
            "shop": {"target": 1, "desc": "Buy from Shop", "reward": 100000}
        }
    },
    "weekly": {
        "duration": 604800, # 7 Days
        "bonus": 1000000,
        "tasks": {
            "msgs": {"target": 1000, "desc": "Chat Messages", "reward": 550000},
            "login_days": {"target": 7, "desc": "Login Days", "reward": 100000},
            "trade": {"target": 8, "desc": "Trades Completed", "reward": 125000},
            "event": {"target": 10, "desc": "Events Participated", "reward": 130000},
            "giveaway": {"target": 7, "desc": "Giveaways Entered", "reward": 150000},
            "shop": {"target": 5, "desc": "Shop Purchases", "reward": 100000}
        }
    },
    "monthly": {
        "duration": 2592000, # 30 Days
        "bonus": 2500000,
        "tasks": {
            "msgs": {"target": 5000, "desc": "Chat Messages", "reward": 2000000},
            "login_days": {"target": 30, "desc": "Login Days", "reward": 1000000},
            "trade": {"target": 25, "desc": "Trades Completed", "reward": 1500000},
            "event": {"target": 30, "desc": "Events Participated", "reward": 1300000},
            "giveaway": {"target": 30, "desc": "Giveaways Entered", "reward": 1500000},
            "shop": {"target": 15, "desc": "Shop Purchases", "reward": 1000000},
            "duelist_club": {"target": 1, "desc": "Register Duelist/Buy Club", "reward": 1000000},
            "shares": {"target": 1, "desc": "Buy/Sell Shares", "reward": 500000}
        }
    },
    "yearly": {
        "duration": 31536000, # 365 Days
        "bonus": 500000000,
        "tasks": {
            "msgs": {"target": 70000, "desc": "Chat Messages", "reward": 250000000},
            "login_days": {"target": 365, "desc": "Login Days", "reward": 100000000},
            "trade": {"target": 150, "desc": "Trades Completed", "reward": 1500000},
            "event": {"target": 1500, "desc": "Events Participated", "reward": 150000000},
            "giveaway": {"target": 500, "desc": "Giveaways Entered", "reward": 150000000},
            "shop": {"target": 150, "desc": "Shop Purchases", "reward": 100000000},
            "invest": {"target": 50000000, "desc": "Club/Share Value Bought", "reward": 100000000}
        }
    },
    "career": {
        "duration": None, # Never resets
        "bonus": 2000000000,
        "tasks": {
            "msgs": {"target": 150000, "desc": "Chat Messages", "reward": 1000000000},
            "login_streak": {"target": 150, "desc": "Login Streak", "reward": 50000000},
            "trade": {"target": 50, "desc": "Trades Completed", "reward": 25000000},
            "event": {"target": 50, "desc": "Events Participated", "reward": 15000000},
            "giveaway": {"target": 150, "desc": "Giveaways Entered", "reward": 15000000},
            "shop": {"target": 150, "desc": "Shop Purchases", "reward": 80000000},
            "club_val": {"target": 150000000, "desc": "Buy Club Worth 150m", "reward": 300000000},
            "share_val": {"target": 100000000, "desc": "Share Trade Volume", "reward": 150000000}
        }
    }
}

def get_quest_data(user_id):
    """Fetch or create quest data for user, handling resets."""
    uid = str(user_id)
    data = quests_col.find_one({"user_id": uid})
    now = datetime.now()
    
    if not data:
        data = {
            "user_id": uid,
            "daily": {"start": now, "claimed_bonus": False, "tasks": {}},
            "weekly": {"start": now, "claimed_bonus": False, "tasks": {}},
            "monthly": {"start": now, "claimed_bonus": False, "tasks": {}},
            "yearly": {"start": now, "claimed_bonus": False, "tasks": {}},
            "career": {"start": now, "claimed_bonus": False, "tasks": {}}
        }
        quests_col.insert_one(data)
        return data

    # Check Resets
    updates = {}
    for q_type in ["daily", "weekly", "monthly", "yearly"]:
        start_time = data[q_type]["start"]
        if not isinstance(start_time, datetime): start_time = now
        
        duration = QUEST_CONFIG[q_type]["duration"]
        if (now - start_time).total_seconds() > duration:
            # Reset this category
            updates[f"{q_type}"] = {"start": now, "claimed_bonus": False, "tasks": {}}
    
    if updates:
        quests_col.update_one({"user_id": uid}, {"$set": updates})
        data.update(updates)
        
    return data

async def update_quest(user_id, task_key, amount=1):
    """Updates progress for a specific task across all applicable timeframes."""
    data = get_quest_data(user_id)
    updates = {}
    
    for q_type in ["daily", "weekly", "monthly", "yearly", "career"]:
        cfg = QUEST_CONFIG[q_type]["tasks"]
        
        # Check if this task exists in this timeframe
        if task_key in cfg:
            current = data[q_type]["tasks"].get(task_key, 0)
            target = cfg[task_key]["target"]
            
            new_amount = current + amount
            updates[f"{q_type}.tasks.{task_key}"] = new_amount
            
            # Auto-Claim Task Reward
            claimed_key = f"claimed_{task_key}"
            is_claimed = data[q_type].get("claimed", {}).get(task_key, False)
            
            if new_amount >= target and not is_claimed:
                reward = cfg[task_key]["reward"]
                wallets_col.update_one({"user_id": str(user_id)}, {"$inc": {"balance": reward}})
                updates[f"{q_type}.claimed.{task_key}"] = True

    if updates:
        quests_col.update_one({"user_id": str(user_id)}, {"$set": updates})

async def show_quest_menu(ctx, q_type):
    data = get_quest_data(ctx.author.id)
    q_data = data[q_type]
    cfg = QUEST_CONFIG[q_type]
    
    # Time Remaining
    if q_type == "career":
        time_str = "∞ (Lifetime)"
    else:
        end_time = q_data["start"] + timedelta(seconds=cfg["duration"])
        time_str = f"<t:{int(end_time.timestamp())}:R>"

    desc = f"**Time Remaining:** {time_str}\n\n"
    
    tasks_completed = 0
    total_tasks = len(cfg["tasks"])
    
    for key, info in cfg["tasks"].items():
        current = q_data["tasks"].get(key, 0)
        target = info["target"]
        reward = info["reward"]
        # Safe check for claimed status
        claimed = q_data.get("claimed", {}).get(key, False)
        
        # Progress Bar
        pct = min(1.0, current / target) if target > 0 else 0
        bar_len = 8
        fill = int(pct * bar_len)
        bar = "🟦" * fill + "⬜" * (bar_len - fill)
        
        status_text = f"**{E_GOLD_TICK} Claimed**" if claimed else (f"**{E_ACTIVE} Ready**" if current >= target else f"`{current}/{target}`")
        
        desc += f"**{info['desc']}**\n{bar} {status_text} | {E_MONEY} ${reward:,}\n\n"
        
        if current >= target: tasks_completed += 1

   # Bonus Reward Status
    bonus_claimed = q_data.get("claimed_bonus", False)
    if tasks_completed >= total_tasks and not bonus_claimed:
        # Auto Claim Bonus
        updates = {"balance": cfg["bonus"]}
        bonus_text = f"Sent: {E_MONEY} **${cfg['bonus']:,}**"
        
        # Add a PC Box ONLY if it is the daily quest
        if q_type == "daily":
            updates["pc_boxes"] = 1
            bonus_text += f"\n{E_ITEMBOX} **Bonus Item:** 1x PC Box"
            
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": updates}, upsert=True)
        quests_col.update_one({"user_id": str(ctx.author.id)}, {"$set": {f"{q_type}.claimed_bonus": True}})
        
        desc += f"\n\n{E_GIVEAWAY} **BONUS UNLOCKED!**\n{bonus_text}"
    elif bonus_claimed:
         desc += f"\n\n{E_GOLD_TICK} **Bonus Claimed:** {E_MONEY} ${cfg['bonus']:,}"
    else:
         desc += f"\n\n{E_CROWN} **Completion Bonus:** {E_MONEY} ${cfg['bonus']:,}"

    # ---------------------------------------------------------
    # THIS IS THE CRITICAL PART THAT LIKELY GOT DELETED:
    # ---------------------------------------------------------
    title_map = {
        "daily": f"{E_STARS} Daily Quests",
        "weekly": f"{E_BOOK} Weekly Quests",
        "monthly": f"{E_CROWN} Monthly Quests",
        "yearly": f"{E_GIVEAWAY} Yearly Quests",
        "career": f"{E_ADMIN} Career Quests"
    }
    
    embed = create_embed(title_map.get(q_type, "Quests"), desc, 0x3498db)
    if ctx.author.avatar:
        embed.set_thumbnail(url=ctx.author.avatar.url)
        
    await ctx.send(embed=embed)
    
# Commands
@bot.hybrid_command(name="dailyquest", aliases=["dq"], description="View Daily Quests.")
async def dailyquest(ctx): await show_quest_menu(ctx, "daily")

@bot.hybrid_command(name="weeklyquest", aliases=["wq"], description="View Weekly Quests.")
async def weeklyquest(ctx): await show_quest_menu(ctx, "weekly")

@bot.hybrid_command(name="monthlyquest", aliases=["mq"], description="View Monthly Quests.")
async def monthlyquest(ctx): await show_quest_menu(ctx, "monthly")

@bot.hybrid_command(name="yearlyquest", aliases=["yq"], description="View Yearly Quests.")
async def yearlyquest(ctx): await show_quest_menu(ctx, "yearly")

@bot.hybrid_command(name="careerquest", aliases=["cq"], description="View Career Quests.")
async def careerquest(ctx): await show_quest_menu(ctx, "career")

@bot.hybrid_command(name="questcomplete", description="Admin: Manually complete an event quest for a user.")
@commands.has_permissions(administrator=True)
async def questcomplete(ctx, event_type: str, member: discord.Member):
    if event_type not in ["event", "giveaway"]:
        return await ctx.send(embed=create_embed("Error", "Type must be 'event' or 'giveaway'.", 0xff0000))
    
    await update_quest(member.id, event_type, 1)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Updated", f"Added +1 {event_type} progress for {member.mention}.", 0x2ecc71))

@bot.hybrid_command(name="event_credit", aliases=["ec", "event"], description="Admin: Give event participation credit to multiple users.")
@commands.has_permissions(administrator=True)
async def event_credit(ctx, members: commands.Greedy[discord.Member]):
    """
    Usage: .ec @User1 @User2 @User3
    Gives +1 Event Participation count to all mentioned users.
    """
    if not members:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Please mention the participants.\nExample: `.ec @User1 @User2`", 0xff0000))
    
    count = 0
    processed_users = []
    
    for m in members:
        if not m.bot:
            await update_quest(m.id, "event", 1)
            processed_users.append(m.display_name)
            count += 1
    
    # Format list for embed (truncate if too long)
    user_list = ", ".join(processed_users)
    if len(user_list) > 100: user_list = user_list[:100] + "..."
    
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Event Credited", f"Added **+1 Event** progress to **{count}** users.\n\n**Users:** {user_list}", 0x2ecc71))
    
# bot.py Part 2 of 4 - Club Market, Auctions & Football
# ... (Continued from Part 1)

# ===========================
#   GROUP 2: CLUB MARKET
# ===========================

def min_required_bid(current):
    add = current * MIN_INCREMENT_PERCENT / 100
    return int(current + max(1, round(add)))

def get_current_bid(item_type=None, item_id=None):
    if db is None: return 0
    if item_type and item_id is not None:
        bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
        if bid: return bid["amount"]
    if item_type == "club":
        c = clubs_col.find_one({"id": int(item_id)})
        return c["base_price"] if c else 0
    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        return d["base_price"] if d else 0
    return 0

async def finalize_auction(item_type: str, item_id: int, channel_id: int):
    if db is None: return
    winner_bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
    channel = bot.get_channel(channel_id)
    club_item = clubs_col.find_one({"id": int(item_id)}) if item_type == "club" else None
    
    if winner_bid:
        bidder_str = winner_bid["bidder"]
        amount = int(winner_bid["amount"])
        if bidder_str.startswith('group:'):
            gname = bidder_str.replace('group:', '').lower()
            groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
        else:
            wallets_col.update_one({"user_id": bidder_str}, {"$inc": {"balance": -amount}})
            log_user_activity(bidder_str, "Transaction", f"Paid ${amount:,} for Auction {item_type} {item_id}")
            
        if item_type == "club":
            old_owner = club_item.get("owner_id")
            if old_owner and not old_owner.startswith("group:"):
                profiles_col.update_one({"user_id": old_owner}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
                past_entities_col.insert_one({"user_id": str(old_owner), "type": "ex_owner", "name": club_item["name"], "timestamp": datetime.now()})
            history_col.insert_one({"club_id": int(item_id), "winner": bidder_str, "amount": amount, "timestamp": datetime.now(), "market_value_at_sale": club_item.get("value", 0)})
            clubs_col.update_one({"id": int(item_id)}, {"$set": { "owner_id": bidder_str, "last_bid_price": amount, "value": amount, "ex_owner_id": old_owner }})
            if not bidder_str.startswith('group:'):
                profiles_col.update_one({"user_id": bidder_str}, {"$set": {"owned_club_id": int(item_id), "owned_club_share": 100}}, upsert=True)
                log_user_activity(bidder_str, "Win", f"Won Auction for Club {club_item['name']}")
            if channel:
                await channel.send(embed=create_embed(f"{E_GIVEAWAY} AUCTION SOLD", f"{E_SUCCESS} **New Owner:** {bidder_str}\n{E_ITEMBOX} **Club:** {club_item['name']}\n{E_MONEY} **Final Price:** ${amount:,}\n{E_STARS} **New Market Value:** ${amount:,}", 0xf1c40f, thumbnail=club_item.get("logo")))
        else: 
            d_item = duelists_col.find_one({"id": int(item_id)})
            salary = d_item["expected_salary"]
            contracts_col.insert_one({"duelist_id": int(item_id), "club_owner": bidder_str, "purchase_price": amount, "salary": salary, "signed_at": datetime.now()})
            target_club_id = None
            if bidder_str.startswith('group:'):
                gname = bidder_str.replace('group:', '').lower()
                c = clubs_col.find_one({"owner_id": f"group:{gname}"})
                if c: target_club_id = c['id']
            else:
                c = clubs_col.find_one({"owner_id": bidder_str})
                if c: target_club_id = c['id']
            duelists_col.update_one({"id": int(item_id)}, {"$set": {"owned_by": bidder_str, "club_id": target_club_id}})
            wallets_col.update_one({"user_id": d_item["discord_user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
            log_user_activity(d_item["discord_user_id"], "Transaction", f"Received ${amount:,} Signing Fee.")
            if channel:
                 await channel.send(embed=create_embed(f"{E_GIVEAWAY} DUELIST SIGNED", f"{E_SUCCESS} **Signed To:** {bidder_str}\n{E_ITEMBOX} **Player:** {d_item['username']}\n{E_MONEY} **Transfer Fee:** ${amount:,}", 0x9b59b6, thumbnail=d_item.get('avatar_url')))
    else:
        if channel: await channel.send(embed=create_embed(f"{E_TIMER} Auction Ended", "No bids were placed.", color=0x95a5a6))
    bids_col.delete_many({"item_type": item_type, "item_id": int(item_id)})
    active_timers.pop((item_type, str(item_id)), None)

def schedule_auction_timer(item_type: str, item_id: int, channel_id: int):
    key = (item_type, str(item_id))
    if active_timers.get(key) and not active_timers.get(key).done(): active_timers[key].cancel()
    loop = asyncio.get_event_loop()
    t = loop.create_task(asyncio.sleep(TIME_LIMIT))
    async def wrapper():
        try: await t; await finalize_auction(item_type, item_id, channel_id)
        except asyncio.CancelledError: return
    active_timers[key] = loop.create_task(wrapper())

@bot.hybrid_command(name="placebid", aliases=["pb"], description="Place a bid.")
async def placebid(ctx, amount: HumanInt, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", f"{E_DANGER} Auctions frozen.", 0xff0000))
    item_type = item_type.lower()
    
    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        is_active = (item_type, str(item_id)) in active_timers
        if d.get("owned_by") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This duelist is already signed.", 0xff0000))
        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
        allowed = False
        if str(ctx.author.id) == c.get("owner_id"): allowed = True
        elif c.get("owner_id", "").startswith("group:"):
            gname = c.get("owner_id").replace("group:", "")
            if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): allowed = True
        if not allowed: return await ctx.send(embed=create_embed("Error", "You/Group don't own this club.", 0xff0000))
    
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
         is_active = (item_type, str(item_id)) in active_timers
         if c.get("owner_id") and not is_active: return await ctx.send(embed=create_embed("Sold Out", f"{E_ERROR} This club is **SOLD OUT**. Wait for owner to sell.", 0xff0000))
         prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
         if prof and prof.get("owned_club_id"): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You already own a club (100%). Sell it first.", 0xff0000))
    
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    req = min_required_bid(get_current_bid(item_type, item_id))
    if amount < req: return await ctx.send(embed=create_embed("Bid Error", f"Min bid is ${req:,}", 0xff0000))
    
    bids_col.insert_one({"bidder": str(ctx.author.id), "amount": amount, "item_type": item_type, "item_id": int(item_id), "timestamp": datetime.now()})
    log_user_activity(ctx.author.id, "Bid", f"Placed bid of ${amount:,} on {item_type} {item_id}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Bid Placed", f"Bid of **${amount:,}** accepted.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="groupbid", aliases=["gb"], description="Place a bid using group funds.")
async def groupbid(ctx, group_name: str, amount: HumanInt, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", "Auctions frozen.", 0xff0000))
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         is_active = (item_type, str(item_id)) in active_timers
         if c.get("owner_id") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This club is **SOLD OUT**.", 0xff0000))
         if clubs_col.find_one({"owner_id": f"group:{gname}"}): return await ctx.send(embed=create_embed("Error", "Group already owns a club.", 0xff0000))
    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        is_active = (item_type, str(item_id)) in active_timers
        if d.get("owned_by") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This duelist is signed.", 0xff0000))
        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c or c.get("owner_id") != f"group:{gname}": return await ctx.send(embed=create_embed("Error", "Group doesn't own club.", 0xff0000))
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    bids_col.insert_one({"bidder": f"group:{gname}", "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    log_user_activity(ctx.author.id, "Bid", f"Group bid ${amount:,} on {item_type} {item_id}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Bid", f"Group **{group_name}** bid **${amount:,}**.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="sellclub", aliases=["sc"], description="Sell your club.")
async def sellclub(ctx, club_name: str, buyer: discord.Member = None):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    if str(ctx.author.id) != c.get("owner_id"): return await ctx.send(embed=create_embed("Error", "You don't own this.", 0xff0000))
    val = c["value"]
    target = buyer if buyer else "The Market"
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Sale", f"Sell **{c['name']}** to {target.mention if buyer else 'Market'} for **${val:,}**?\nType `yes` or `no`.", 0xe67e22))
    try: msg = await bot.wait_for('message', check=lambda m: m.author == (buyer if buyer else ctx.author) and m.content.lower() in ['yes', 'no'], timeout=30.0)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    if msg.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled.", 0x95a5a6))
    old_owner = c.get("owner_id")
    if old_owner:
        profiles_col.update_one({"user_id": old_owner}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        log_past_entity(old_owner, "ex_owner", c['name'])
    if buyer:
        bw = wallets_col.find_one({"user_id": str(buyer.id)})
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": str(buyer.id), "ex_owner_id": old_owner}})
        profiles_col.update_one({"user_id": str(buyer.id)}, {"$set": {"owned_club_id": c["id"], "owned_club_share": 100}}, upsert=True)
    else:
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": None, "ex_owner_id": old_owner}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
    embed_log = create_embed(f"{E_ADMIN} Club Sold", f"**Club:** {c['name']}\n**Seller:** {ctx.author.mention}\n**Buyer:** {target.mention if buyer else 'Market'}\n**Price:** ${val:,}", 0xe67e22)
    await send_log("club", embed_log)
    log_user_activity(ctx.author.id, "Sale", f"Sold club {c['name']} for ${val:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", f"Club sold for **${val:,}**.", 0x2ecc71))

@bot.hybrid_command(name="sellshares", aliases=["ss"], description="Sell group shares.")
async def sellshares(ctx, club_name: str, buyer: discord.Member, percentage: int):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    owner_str = c.get("owner_id", "")
    if not owner_str.startswith("group:"): return await ctx.send(embed=create_embed("Error", "Not group owned.", 0xff0000))
    gname = owner_str.replace("group:", "")
    seller = group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)})
    if not seller or seller["share_percentage"] < percentage: return await ctx.send(embed=create_embed("Error", "Not enough shares.", 0xff0000))
    val = int(c["value"] * (percentage / 100))
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Share Sale", f"{buyer.mention}, buy **{percentage}%** shares for **${val:,}**? `yes`/`no`", 0xe67e22))
    try: msg = await bot.wait_for('message', check=lambda m: m.author == buyer and m.content.lower() in ['yes', 'no'], timeout=30)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    if msg.content.lower() == 'yes':
        bw = wallets_col.find_one({"user_id": str(buyer.id)})
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
        group_members_col.update_one({"_id": seller["_id"]}, {"$inc": {"share_percentage": -percentage}})
        group_members_col.update_one({"group_name": gname, "user_id": str(buyer.id)}, {"$inc": {"share_percentage": percentage}}, upsert=True)
        log_user_activity(ctx.author.id, "Sale", f"Sold {percentage}% shares of {gname}.")
        try:
            await update_quest(ctx.author.id, "shares", 1) # For Seller
            await update_quest(buyer.id, "shares", 1)      # For Buyer
        except: pass
        # ---------------------------
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", "Shares transferred.", 0x2ecc71))

@bot.command(name="marketlist", aliases=["ml"], description="View unsold clubs.")
async def marketlist(ctx):
    unsold_clubs = list(clubs_col.find({"$or": [{"owner_id": None}, {"owner_id": ""}]}).sort("value", -1))
    if not unsold_clubs: return await ctx.send(embed=create_embed(f"{E_AUCTION} Market Empty", "All clubs are currently owned.", 0x95a5a6))
    data = []
    for c in unsold_clubs: data.append((f"{E_STAR} {c['name']}", f"{E_MONEY} **Price:** ${c['value']:,}\n{E_BOOST} **Division:** {c.get('level_name', 'Unknown')}\n{E_ITEMBOX} **ID:** {c['id']}"))
    view = Paginator(ctx, data, f"{E_AUCTION} Transfer Market", 0xe67e22, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="buyclub", aliases=["bc"], description="Request to buy a club.")
async def buyclub(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    if c.get("owner_id"): return await ctx.send(embed=create_embed("Error", f"{E_DANGER} Already owned.", 0xff0000))
    prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
    if prof and prof.get("owned_club_id"): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You already own a club.", 0xff0000))
    price = c["value"]
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < price: return await ctx.send(embed=create_embed("Insufficient Funds", f"{E_ERROR} Need **${price:,}**.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -price}})
    deal_id = get_next_id("deal_id")
    pending_deals_col.insert_one({"id": deal_id, "type": "user", "buyer_id": str(ctx.author.id), "club_id": c["id"], "club_name": c["name"], "price": price, "timestamp": datetime.now()})
    # 👇 QUEST HOOK ADDED HERE 👇
    try: await update_quest(ctx.author.id, "duelist_club", 1)
    except: pass
    # ---------------------------
    embed = discord.Embed(title=f"{E_TIMER} Deal Pending", description=f"Request to buy **{c['name']}** submitted.\n\n{E_MONEY} **Funds Held:** ${price:,}\n{E_ADMIN} **Status:** Waiting for Admin Approval\n{E_ITEMBOX} **Deal ID:** {deal_id}", color=0xf1c40f)
    if c.get("logo"): embed.set_thumbnail(url=c["logo"])
    await ctx.send(embed=embed)

@bot.hybrid_command(name="groupbuyclub", aliases=["gbc"], description="Request to buy club for group.")
async def groupbuyclub(ctx, group_name: str, club_name: str):
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Group not found.", 0xff0000))
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Not a member.", 0xff0000))
    if clubs_col.find_one({"owner_id": f"group:{gname}"}): return await ctx.send(embed=create_embed("Error", f"{E_DANGER} Group already owns a club.", 0xff0000))
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    if c.get("owner_id"): return await ctx.send(embed=create_embed("Error", f"{E_DANGER} Already owned.", 0xff0000))
    price = c["value"]
    if g.get("funds", 0) < price: return await ctx.send(embed=create_embed("Insufficient Funds", f"{E_ERROR} Group needs **${price:,}**.", 0xff0000))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -price}})
    deal_id = get_next_id("deal_id")
    pending_deals_col.insert_one({"id": deal_id, "type": "group", "buyer_id": f"group:{gname}", "initiator_id": str(ctx.author.id), "club_id": c["id"], "club_name": c["name"], "price": price, "timestamp": datetime.now()})
    embed = discord.Embed(title=f"{E_TIMER} Group Deal Pending", description=f"Request to buy **{c['name']}** for **{group_name}** submitted.\n\n{E_MONEY} **Funds Held:** ${price:,}\n{E_ADMIN} **Status:** Waiting for Admin Approval\n{E_ITEMBOX} **Deal ID:** {deal_id}", color=0xf1c40f)
    if c.get("logo"): embed.set_thumbnail(url=c["logo"])
    await ctx.send(embed=embed)

@bot.hybrid_command(name="marketpanel", aliases=["mp"], description="View market stats.")
async def marketpanel(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    cur_lvl, nxt_lvl, req_wins = get_level_info(c.get('total_wins', 0), c.get('level_name'))
    embed = discord.Embed(title=f"{E_STARS} Market Panel: {c['name']}", color=0xf1c40f)
    if c.get("logo"): embed.set_thumbnail(url=c["logo"])
    embed.add_field(name="Market Value", value=f"{E_MONEY} **${c['value']:,}**", inline=True)
    embed.add_field(name="Total Wins", value=f"{E_FIRE} **{c.get('total_wins', 0)}**", inline=True)
    embed.add_field(name="Division", value=f"{E_CROWN} **{cur_lvl}**", inline=False)
    if nxt_lvl: embed.add_field(name="Next", value=f"{E_ARROW} **{nxt_lvl[0]}** (Req: {req_wins} wins)", inline=True)
    else: embed.add_field(name="Status", value=f"{E_GOLD_TICK} Max Level", inline=False)
    await ctx.send(embed=embed)

# ===========================
#   GROUP 3: FOOTBALL
# ===========================

def get_club_owner_info(club_id):
    if db is None: return None, []
    c = clubs_col.find_one({"id": int(club_id)})
    if not c or "owner_id" not in c or not c["owner_id"]: return None, []
    owner_str = c["owner_id"]
    if owner_str.startswith('group:'):
        gname = owner_str.replace('group:', '').lower()
        members = group_members_col.find({"group_name": gname})
        return owner_str, [m['user_id'] for m in members]
    return owner_str, [owner_str]

def get_level_info(current_wins, level_name=None):
    current_level = level_name if level_name else LEVEL_UP_CONFIG[0][1]
    next_level_info = None
    required_wins = 0
    for wins_required, name, bonus in LEVEL_UP_CONFIG:
        if wins_required > current_wins:
            next_level_info = (name, wins_required, bonus)
            required_wins = wins_required - current_wins
            break
        elif wins_required <= current_wins:
            current_level = name
    return current_level, next_level_info, required_wins

def update_club_level(club_id, wins_gained=0):
    if db is None: return
    c = clubs_col.find_one({"id": int(club_id)})
    if not c: return None
    new_wins = c.get("total_wins", 0) + wins_gained
    clubs_col.update_one({"id": int(club_id)}, {"$set": {"total_wins": new_wins}})
    for wins_required, name, bonus in LEVEL_UP_CONFIG:
        if c.get("total_wins", 0) < wins_required <= new_wins:
            clubs_col.update_one({"id": int(club_id)}, {"$set": {"level_name": name}, "$inc": {"value": bonus}})
            return name
    return None

@bot.command(name="listclubs", aliases=["lc"], description="List all registered clubs.")
async def listclubs(ctx):
    clubs = list(clubs_col.find().sort("value", -1))
    data = []
    for c in clubs: data.append((f"{E_STAR} {c['name']} (ID: {c['id']})", f"{E_MONEY} ${c['value']:,} | {E_BOOST} {c.get('level_name')} | {E_FIRE} Wins: {c.get('total_wins',0)}"))
    view = Paginator(ctx, data, f"{E_CROWN} Registered Clubs", 0x3498db, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="clubinfo", aliases=["ci"], description="Get club info.")
async def clubinfo(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    owner_display = c.get('owner_id') or "Unowned"
    manager_name = "None"
    if c.get("manager_id"):
        try: m = await bot.fetch_user(int(c["manager_id"])); manager_name = m.name
        except: manager_name = "Unknown"
    shareholder_text = ""
    if owner_display.startswith('group:'): 
        gname = owner_display.replace('group:', '').title()
        owner_display = f"Group: {gname}"
    else:
        try:
            if owner_display != "Unowned":
                owner_user = await bot.fetch_user(int(owner_display))
                owner_display = f"User: {owner_user.display_name}"
        except: pass
    duelists = list(duelists_col.find({"club_id": c['id']}))
    d_list = "\n".join([f"{E_ARROW} {d['username']}" for d in duelists]) or "None"
    embed = discord.Embed(title=f"{E_CROWN} {c['name']}", description=f"{E_BOOST} **{c.get('level_name')}**{shareholder_text}", color=0x3498db)
    if c.get("logo"): embed.set_thumbnail(url=c["logo"])
    embed.add_field(name="Owner", value=f"{E_STAR} {owner_display}", inline=True)
    embed.add_field(name="Value", value=f"{E_MONEY} ${c['value']:,}", inline=True)
    embed.add_field(name="Manager", value=f"{E_ADMIN} {manager_name}", inline=True)
    embed.add_field(name=f"{E_ITEMBOX} Duelists", value=d_list, inline=False)

# --- CLUB TROPHIES INJECTION ---
    trophies_text = ""
    if c.get("t_ucl", 0) > 0:
        count = c["t_ucl"]
        trophies_text += f"**UCL Titles**\n{count}x " + (E_UCL * count) + "\n\n"
    if c.get("t_league", 0) > 0:
        count = c["t_league"]
        trophies_text += f"**League Titles**\n{count}x " + (E_LEAGUE * count) + "\n\n"
    if c.get("t_supercup", 0) > 0:
        count = c["t_supercup"]
        trophies_text += f"**Super Cups**\n{count}x " + (E_SUPERCUP * count) + "\n\n"
        
    if trophies_text:
        embed.add_field(name=f"{E_STARS} Club Trophy Cabinet", value=trophies_text.strip(), inline=False)
    # -------------------------------
    
    await ctx.send(embed=embed)

@bot.command(name="trend", aliases=["marketnews", "market"], description="View the live club market trends.")
async def trend(ctx):
    # Fetch all clubs that have been processed by the simulation at least once
    clubs = list(clubs_col.find({"previous_value": {"$exists": True}}))
    
    if not clubs:
        return await ctx.send(embed=create_embed("Market Alert", f"{E_ALERT} The market simulation hasn't run yet. Check back in an hour!", 0xf1c40f))
        
    trends = []
    for c in clubs:
        curr = c.get("value", 100000)
        prev = c.get("previous_value", 100000)
        if prev <= 0: prev = 1 # Prevent division by zero
        
        pct = ((curr - prev) / prev) * 100
        trends.append((c['name'], curr, prev, pct))
        
    # Sort clubs by percentage change (Highest to Lowest)
    trends.sort(key=lambda x: x[3], reverse=True)
    
    top_gainers = [t for t in trends if t[3] > 0][:5]
    top_losers = [t for t in trends if t[3] < 0][-5:]
    top_losers.reverse() # Put the most negative at the top of the losers list
    
    desc = f"The live market updates every hour. Here are the biggest movers:\n\n"
    
    if top_gainers:
        desc += f"**{E_SUCCESS} Top Gainers**\n"
        for name, curr, prev, pct in top_gainers:
            desc += f"**{name}** | {E_MONEY} **${curr:,}** `(+{pct:.1f}%)`\n"
            
    if top_losers:
        desc += f"\n**{E_DANGER} Top Losers**\n"
        for name, curr, prev, pct in top_losers:
            desc += f"**{name}** | {E_MONEY} **${curr:,}** `({pct:.1f}%)`\n"
            
    if not top_gainers and not top_losers:
        desc += f"{E_STARS} *The market is completely stable right now.*"
            
    embed = create_embed(f"{E_BOOK} Live Market Trends", desc, 0x3498db)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="clublevel", aliases=["cl"], description="Check club level.")
async def clublevel(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    cur, nxt, req = get_level_info(c.get('total_wins', 0), c.get('level_name'))
    embed = create_embed(f"{E_BOOST} Club Level", f"**{c['name']}**\n{E_CROWN} Current: **{cur}**\n{E_FIRE} Wins: **{c.get('total_wins',0)}**", 0xf1c40f)
    if nxt: embed.add_field(name="Next Level", value=f"{E_ARROW} **{nxt[0]}**\n{E_RED_ARROW} Needs **{req}** wins")
    else: embed.add_field(name="Status", value=f"{E_GOLD_TICK} Max Level")
    await ctx.send(embed=embed)

@bot.command(name="leaderboard", aliases=["lb"], description="View top clubs.")
async def leaderboard(ctx):
    clubs = list(clubs_col.find().sort([("total_wins", -1), ("value", -1)]))
    data = []
    for i, c in enumerate(clubs): data.append((f"**{i+1}. {c['name']}**", f"{E_ARROW} {c.get('level_name')} | {E_FIRE} {c.get('total_wins')} Wins | {E_MONEY} ${c['value']:,}"))
    view = Paginator(ctx, data, f"{E_CROWN} Club Leaderboard", 0xf1c40f, 10)
    await ctx.send(embed=view.get_embed(), view=view)


# ==============================================================================
#  TRADE SYSTEM (Global State & Views)
# ==============================================================================

# Global Trade State Storage
active_trades = {}

class TradeSession:
    def __init__(self, initiator_id, target_id, channel_id):
        self.users = [initiator_id, target_id]
        self.channel_id = channel_id
        self.offers = {
            initiator_id: {"cash": 0, "sc": 0, "items": {}}, 
            target_id: {"cash": 0, "sc": 0, "items": {}}
        }
        self.confirmed = {initiator_id: False, target_id: False}
        self.finalized = False

class TradeInviteView(View):
    def __init__(self, initiator, target):
        super().__init__(timeout=60)
        self.initiator = initiator
        self.target = target
        self.accepted = False

    @discord.ui.button(label="Accept Trade", style=discord.ButtonStyle.success, emoji=E_GOLD_TICK)
    async def accept(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.target.id:
            return await interaction.response.send_message("This trade request is not for you.", ephemeral=True)
        
        self.accepted = True
        # Initialize Session
        session = TradeSession(str(self.initiator.id), str(self.target.id), interaction.channel.id)
        active_trades[str(self.initiator.id)] = session
        active_trades[str(self.target.id)] = session
        
        embed = create_embed(f"{E_AUCTION} Trade Active", 
                             f"Trade started between {self.initiator.mention} and {self.target.mention}!\n\n"
                             f"**Commands:**\n"
                             f"`.trade add $ 500` (Add Cash)\n"
                             f"`.trade add sc 100` (Add Shiny Coins)\n"
                             f"`.trade add inv <Item Name>` (Add Item/Pokemon)\n"
                             f"`.trade remove ...` (Remove anything)\n"
                             f"`.trade confirm` (Lock in your offer)", 
                             0x2ecc71)
        
        await interaction.response.edit_message(content=None, embed=embed, view=None)
        self.stop()

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger, emoji=E_ERROR)
    async def reject(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.target.id and interaction.user.id != self.initiator.id: return
        await interaction.response.edit_message(content=f"{E_ERROR} Trade rejected/cancelled.", embed=None, view=None)
        self.stop()

class TradeFinalView(View):
    def __init__(self, session):
        super().__init__(timeout=60)
        self.session = session

    @discord.ui.button(label="Confirm Deal", style=discord.ButtonStyle.success, emoji=E_GOLD_TICK)
    async def confirm_deal(self, interaction: discord.Interaction, button: Button):
        uid = str(interaction.user.id)
        if uid not in self.session.users: return
        
        # Mark user as final confirmed (reuse the confirmed dict or add a new flag, here we just check logic inside)
        # For simplicity, we process immediately if both click, or wait. 
        # But to prevent race conditions, we'll execute logic only once.
        
        if self.session.finalized: return await interaction.response.send_message("Trade already finalizing...", ephemeral=True)
        
        # We need a way to track double clicks on this specific view. 
        # Let's use the button's style or a local list.
        if not hasattr(self, 'final_clicks'): self.final_clicks = []
        if uid in self.final_clicks: return await interaction.response.send_message("You already confirmed.", ephemeral=True)
        
        self.final_clicks.append(uid)
        await interaction.response.send_message(f"{E_SUCCESS} {interaction.user.display_name} confirmed!", ephemeral=True)

        if len(self.final_clicks) == 2:
            self.session.finalized = True
            await self.execute_trade(interaction)

    async def execute_trade(self, interaction):
        # 1. Re-verify everything (Security Check)
        for uid in self.session.users:
            offer = self.session.offers[uid]
            w = get_wallet(uid)
            
            # Check Currency
            if w.get("balance", 0) < offer["cash"]: return await interaction.channel.send(f"{E_ERROR} <@{uid}> is missing Cash funds! Trade Cancelled.")
            if w.get("shiny_coins", 0) < offer["sc"]: return await interaction.channel.send(f"{E_ERROR} <@{uid}> is missing Shiny Coins! Trade Cancelled.")
                
            # Check Items
            for item_name, qty_needed in offer["items"].items():
                inv_item = inventory_col.find_one({"user_id": uid, "name": item_name})
                if not inv_item or inv_item.get("quantity", 0) < qty_needed:
                    return await interaction.channel.send(f"{E_ERROR} <@{uid}> is missing **{item_name}**! Trade Cancelled.")

        # 2. Execute Transfers
        u1, u2 = self.session.users[0], self.session.users[1]
        
        # Function to transfer assets from sender to receiver
        def transfer_assets(sender, receiver):
            offer = self.session.offers[sender]
            
            # Currency
            if offer["cash"] > 0:
                wallets_col.update_one({"user_id": sender}, {"$inc": {"balance": -offer["cash"]}})
                wallets_col.update_one({"user_id": receiver}, {"$inc": {"balance": offer["cash"]}}, upsert=True)
            if offer["sc"] > 0:
                wallets_col.update_one({"user_id": sender}, {"$inc": {"shiny_coins": -offer["sc"]}})
                wallets_col.update_one({"user_id": receiver}, {"$inc": {"shiny_coins": offer["sc"]}}, upsert=True)
            
            # Items
            for item_name, qty in offer["items"].items():
                # Remove from Sender
                sender_item = inventory_col.find_one({"user_id": sender, "name": item_name})
                inventory_col.update_one({"_id": sender_item["_id"]}, {"$inc": {"quantity": -qty}})
                
                # Add to Receiver
                # We need the item_id and type to upsert correctly.
                item_id = sender_item['item_id']
                item_type = sender_item.get('type', 'Item')
                
                # If Pokemon, we might need to update ownership in pokemon_col too if unique
                # Assuming inventory quantity based, we just move quantity.
                inventory_col.update_one(
                    {"user_id": receiver, "item_id": item_id},
                    {"$inc": {"quantity": qty}, "$set": {"name": item_name, "type": item_type}},
                    upsert=True
                )
                
                # Special Pokemon Owner Update (If tracking unique ownership)
                if item_type == "Pokemon":
                    # Update owner_id in pokemon_col for these specific instances? 
                    # Complex with quantity. For this system, we rely on inventory_col ownership.
                    pass

        # Execute Swap
        transfer_assets(u1, u2)
        transfer_assets(u2, u1)

        await update_quest(u1, "trade", 1)
        await update_quest(u2, "trade", 1)

        # Cleanup
        del active_trades[u1]
        del active_trades[u2]
        
        await interaction.channel.send(embed=create_embed(f"{E_SUCCESS} Trade Complete!", "All items and currencies have been transferred.", 0x2ecc71))
        self.stop()
        # --- LOGGING TO HISTORY (Add this block) ---
        
        # Helper to stringify an offer
        def offer_to_str(uid):
            o = self.session.offers[uid]
            parts = []
            if o["cash"]: parts.append(f"${o['cash']:,}")
            if o["sc"]: parts.append(f"{o['sc']:,} SC")
            for n, q in o["items"].items(): parts.append(f"{n} x{q}")
            return ", ".join(parts) if parts else "Nothing"

        u1_offer_str = offer_to_str(u1)
        u2_offer_str = offer_to_str(u2)

        db.trade_history.insert_one({
            "users": [u1, u2],
            "offers": {u1: u1_offer_str, u2: u2_offer_str},
            "summary": f"[{u1_offer_str}] ↔️ [{u2_offer_str}]",
            "timestamp": datetime.now()
        })
        
        # --- END LOGGING BLOCK ---

    @discord.ui.button(label="Cancel Trade", style=discord.ButtonStyle.danger, emoji=E_ERROR)
    async def cancel_trade(self, interaction: discord.Interaction, button: Button):
        for uid in self.session.users:
            if uid in active_trades: del active_trades[uid]
        await interaction.response.edit_message(content=f"{E_DANGER} Trade cancelled by {interaction.user.mention}.", embed=None, view=None)
        self.stop()

# ==============================================================================
#  TRADE COMMAND GROUP
# ==============================================================================

@bot.group(name="trade", invoke_without_command=True)
async def trade(ctx, target: discord.Member):
    """Start a trade with another user."""
    if target.id == ctx.author.id: return await ctx.send(embed=create_embed("Error", "You cannot trade with yourself.", 0xff0000))
    if target.bot: return await ctx.send(embed=create_embed("Error", "You cannot trade with bots.", 0xff0000))
    
    if str(ctx.author.id) in active_trades: return await ctx.send(embed=create_embed("Error", "You are already in a trade.", 0xff0000))
    if str(target.id) in active_trades: return await ctx.send(embed=create_embed("Error", f"{target.display_name} is already in a trade.", 0xff0000))

    view = TradeInviteView(ctx.author, target)
    await ctx.send(f"{target.mention}, {ctx.author.mention} wants to trade with you!", view=view)

@trade.command(name="add")
async def trade_add(ctx, category: str, *, item_or_amount: str):
    """Add items or currency. Usage: .trade add $ 500 | .trade add inv Pikachu"""
    uid = str(ctx.author.id)
    if uid not in active_trades: return await ctx.send("You are not in a trade.")
    session = active_trades[uid]
    
    # 1. Parse Input
    category = category.lower()
    
    # --- CURRENCY ---
    if category in ["$", "money", "cash"]:
        try: amount = int(item_or_amount.replace(",", "").replace("k", "000").replace("m", "000000"))
        except: return await ctx.send("Invalid amount.")
        if amount <= 0: return
        
        w = get_wallet(ctx.author.id) # This will work now
        current_bal = w.get("balance", 0)
        current_offer = session.offers[uid]["cash"]
        
        if (current_bal - current_offer) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient Cash in wallet.", 0xff0000))
        
        session.offers[uid]["cash"] += amount
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Added", f"Added {E_MONEY} ${amount:,} to trade.", 0x2ecc71))

    elif category in ["sc", "shiny", "coins"]:
        try: amount = int(item_or_amount.replace(",", ""))
        except: return await ctx.send("Invalid amount.")
        if amount <= 0: return
        
        w = get_wallet(ctx.author.id)
        current_bal = w.get("shiny_coins", 0)
        current_offer = session.offers[uid]["sc"]
        
        if (current_bal - current_offer) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient Shiny Coins.", 0xff0000))
        
        session.offers[uid]["sc"] += amount
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Added", f"Added {E_SHINY} {amount:,} SC to trade.", 0x2ecc71))
        
    # --- INVENTORY (Smart Search) ---
    elif category in ["inv", "item", "pokemon"]:
        item_search = item_or_amount.strip()
        
        # FIX: Find item using partial match (case insensitive)
        item = inventory_col.find_one({
            "user_id": uid, 
            "name": {"$regex": re.escape(item_search), "$options": "i"},
            "quantity": {"$gt": 0}
        })
        
        if not item:
            return await ctx.send(embed=create_embed("Error", f"Could not find **{item_search}** in your inventory.", 0xff0000))
        
        # Check quantity limits
        current_offer_qty = session.offers[uid]["items"].get(item['name'], 0)
        if (item['quantity'] - current_offer_qty) < 1:
            return await ctx.send(embed=create_embed("Error", f"You don't have enough **{item['name']}** left.", 0xff0000))
            
        # Add to offer
        if item['name'] in session.offers[uid]["items"]:
            session.offers[uid]["items"][item['name']] += 1
        else:
            session.offers[uid]["items"][item['name']] = 1
            
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Added", f"Added 1x **{item['name']}** to trade.", 0x2ecc71))
        
    else:
        await ctx.send("Invalid category. Use `$`, `sc`, or `inv`.")
        
    session.confirmed = {u: False for u in session.users}

@trade.command(name="remove")
async def trade_remove(ctx, category: str, *, item_or_amount: str):
    """Remove items or currency from trade."""
    uid = str(ctx.author.id)
    if uid not in active_trades: return
    session = active_trades[uid]
    category = category.lower()
    
    if category in ["$", "cash"]:
        try: amount = int(item_or_amount.replace(",", "").replace("k", "000"))
        except: return
        if session.offers[uid]["cash"] >= amount:
            session.offers[uid]["cash"] -= amount
            await ctx.send(embed=create_embed(f"{E_DANGER} Removed", f"Removed ${amount:,} Cash.", 0xff0000))
            
    elif category in ["sc", "shiny"]:
        try: amount = int(item_or_amount.replace(",", ""))
        except: return
        if session.offers[uid]["sc"] >= amount:
            session.offers[uid]["sc"] -= amount
            await ctx.send(embed=create_embed(f"{E_DANGER} Removed", f"Removed {amount:,} SC.", 0xff0000))
            
    elif category in ["inv", "item", "pokemon"]:
        name_search = item_or_amount.strip().lower()
        found_key = None
        
        # Smart find in current offers
        for key in session.offers[uid]["items"]:
            if name_search in key.lower():
                found_key = key
                break
        
        if found_key:
            session.offers[uid]["items"][found_key] -= 1
            if session.offers[uid]["items"][found_key] <= 0:
                del session.offers[uid]["items"][found_key]
            await ctx.send(embed=create_embed(f"{E_DANGER} Removed", f"Removed 1x **{found_key}**.", 0xff0000))
        else:
            await ctx.send("Item not found in current offer.")

    session.confirmed = {u: False for u in session.users}


@trade.command(name="confirm")
async def trade_confirm(ctx):
    """Confirm your side of the trade."""
    uid = str(ctx.author.id)
    if uid not in active_trades: return
    session = active_trades[uid]
    
    session.confirmed[uid] = True
    await ctx.send(embed=create_embed(f"{E_GOLD_TICK} Confirmed", f"{ctx.author.mention} has confirmed the trade offer.", 0xf1c40f))
    
    # Check if both confirmed
    if all(session.confirmed.values()):
        # Show Final Summary
        u1, u2 = session.users[0], session.users[1]
        
        def format_offer(user_id):
            o = session.offers[user_id]
            txt = ""
            if o["cash"] > 0: txt += f"{E_MONEY} ${o['cash']:,}\n"
            if o["sc"] > 0: txt += f"{E_SHINY} {o['sc']:,} SC\n"
            for name, qty in o["items"].items():
                txt += f"{E_ITEMBOX} {name} x{qty}\n"
            return txt if txt else "*Nothing*"

        desc = f"**Trade Summary:**\n\n**<@{u1}> Offers:**\n{format_offer(u1)}\n\n**<@{u2}> Offers:**\n{format_offer(u2)}"
        
        view = TradeFinalView(session)
        await ctx.send(embed=create_embed(f"{E_STARS} Final Confirmation", desc, 0x3498db), view=view)

@trade.command(name="cancel")
async def trade_cancel(ctx):
    uid = str(ctx.author.id)
    if uid in active_trades:
        session = active_trades[uid]
        for u in session.users:
            if u in active_trades: del active_trades[u]
        await ctx.send(embed=create_embed(f"{E_ERROR} Trade Cancelled", "Trade session ended.", 0xff0000))
    else:
        await ctx.send("No active trade.")


@bot.hybrid_command(name="registerduelist", aliases=["rd"], description="Register as duelist.")
async def registerduelist(ctx, username: str, base_price: HumanInt, salary: HumanInt):
    # Check both old and new ID formats to prevent double-registration
    if duelists_col.find_one({"discord_user_id": str(ctx.author.id)}) or duelists_col.find_one({"user_id": str(ctx.author.id)}): 
        return await ctx.send(embed=create_embed("Error", "Already registered.", 0xff0000))
        
    did = get_next_id("duelist_id")
    avatar = ctx.author.avatar.url if ctx.author.avatar else ""
    
    # THE UPGRADED DATABASE ENTRY
    duelists_col.insert_one({
        "id": did, # Kept for legacy commands
        "duelist_id": f"D{did}", # New ID format for esports (e.g. D15)
        "discord_user_id": str(ctx.author.id), # Kept for legacy commands
        "user_id": str(ctx.author.id), # New ID format for esports
        "username": username,
        "base_price": base_price,
        "expected_salary": salary,
        "market_worth": base_price, # NEW: Starting market worth
        "status": "Free Agent", # NEW: Starting status
        "transfer_listed": False, # NEW: Market status
        "wins": 0, "losses": 0, "draws": 0, "matches": 0, "mvps": 0, # NEW: Match Stats
        "avatar_url": avatar, 
        "owned_by": None, 
        "club_id": None
    })
    
    # 👇 QUEST HOOK ADDED HERE 👇
    try: await update_quest(ctx.author.id, "duelist_club", 1)
    except: pass
    # ---------------------------
    
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Registered", f"Duelist **{username}** (ID: D{did}) successfully registered to the Esports Engine!", 0x9b59b6))

@bot.hybrid_command(name="retireduelist", aliases=["ret"], description="Retire a duelist.")
async def retireduelist(ctx, member: discord.Member = None):
    target_id = str(member.id) if member else str(ctx.author.id)
    d = duelists_col.find_one({"user_id": target_id}) # Upgraded to user_id
    if not d: return await ctx.send(embed=create_embed("Error", "Not a duelist.", 0xff0000))
    
    if member:
        if not d.get("club_id"): return await ctx.send(embed=create_embed("Error", "Free agent must self-retire.", 0xff0000))
        c = clubs_col.find_one({"id": d["club_id"]})
        owner_str, owner_ids = get_club_owner_info(c["id"])
        if str(ctx.author.id) not in owner_ids: return await ctx.send(embed=create_embed("Error", "Not owner.", 0xff0000))
        
        await ctx.send(embed=create_embed(f"{E_ALERT} Confirm", f"Owner {ctx.author.mention}, retire {member.mention}? `yes`/`no`", 0xe67e22))
        try: msg = await bot.wait_for('message', check=lambda m: m.author==ctx.author and m.content.lower() in ['yes','no'], timeout=30)
        except: return
        if msg.content.lower() == 'no': return
        
        await ctx.send(embed=create_embed(f"{E_ALERT} Confirm", f"Duelist {member.mention}, confirm retirement? `yes`/`no`", 0xe67e22))
        try: msg2 = await bot.wait_for('message', check=lambda m: m.author==member and m.content.lower() in ['yes','no'], timeout=30)
        except: return
        if msg2.content.lower() == 'no': return
    else:
        if d.get("club_id"): return await ctx.send(embed=create_embed("Error", "You are signed. Ask owner.", 0xff0000))

    # --- NEW CLEANUP LOGIC ---
    # Erase any ghost contracts or transfer market listings
    if db is not None:
        db.contracts.delete_many({"duelist_id": d["_id"]})
        db.pending_contracts.delete_many({"duelist_id": d["_id"]})
        db.pending_transfers.delete_many({"duelist_id": d["_id"]})
    # -------------------------

    duelists_col.delete_one({"_id": d["_id"]})
    
    embed_log = create_embed(f"{E_DANGER} Duelist Retired", f"**Player:** {d['username']}\n**ID:** {d.get('duelist_id')}", 0xff0000)
    await send_log("duelist", embed_log)
    log_user_activity(target_id, "Duelist", "Retired")
    await ctx.send(embed=create_embed(f"{E_DANGER} Retired", f"Duelist **{d['username']}** retired.", 0xff0000))
    
@bot.hybrid_command(name="listduelists", aliases=["ld"], description="List all registered duelists and their market value.")
async def listduelists(ctx):
    ds = list(duelists_col.find({"status": {"$ne": "Left the Server"}})) # Hides people who left
    if not ds: return await ctx.send(embed=create_embed("Empty", f"{E_ERROR} No duelists registered yet.", 0xff0000))
        
    data = []
    for d in ds:
        cname = "Free Agent"
        if d.get("club_id"):
            c = clubs_col.find_one({"id": d["club_id"]}) or clubs_col.find_one({"_id": d["club_id"]})
            if c: cname = c["name"]
            
        # Fetching the sleek new esports values
        duelist_id = d.get('duelist_id', f"D{d.get('id', 'N/A')}")
        worth = d.get('market_worth', d.get('base_price', 0))
        status = d.get('status', 'Free Agent')
        
        desc = (
            f"{E_ITEMBOX} **ID:** `{duelist_id}`\n"
            f"{E_MONEY} **Value:** ${worth:,}\n"
            f"{E_STAR} **Club:** {cname}\n"
            f"{E_ADMIN} **Status:** {status}"
        )
        
        data.append((f"{d.get('username', 'Unknown')}", desc))
        
    view = Paginator(ctx, data, f"{E_BOOK} Global Duelist Registry", 0x9b59b6, 10)
    await ctx.send(embed=view.get_embed(), view=view)
    
@bot.hybrid_command(name="adjustsalary", aliases=["as"], description="Owner: Bonus/Fine.")
async def adjustsalary(ctx, duelist_identifier: str, amount: HumanInt): # Upgraded to identifier
    d = get_duelist(duelist_identifier)
    if not d or not d.get("club_id"): return await ctx.send(embed=create_embed("Error", "Duelist not found/signed.", 0xff0000))
    
    c = clubs_col.find_one({"id": d["club_id"]})
    owner_str, owner_ids = get_club_owner_info(c["id"])
    if str(ctx.author.id) not in owner_ids: return await ctx.send(embed=create_embed("Error", "Not owner.", 0xff0000))
    
    if amount > 0:
        w = wallets_col.find_one({"user_id": str(ctx.author.id)})
        if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
        
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
        wallets_col.update_one({"user_id": d["user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
        log_user_activity(ctx.author.id, "Transaction", f"Paid bonus ${amount:,} to {d['username']}")
        await ctx.send(embed=create_embed(f"{E_MONEY} Bonus", f"Paid **${amount:,}** to {d['username']}.", 0x2ecc71))
    else:
        abs_amt = abs(amount)
        wallets_col.update_one({"user_id": d["user_id"]}, {"$inc": {"balance": -abs_amt}}, upsert=True)
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": abs_amt}}, upsert=True)
        log_user_activity(ctx.author.id, "Transaction", f"Fined {d['username']} ${abs_amt:,}")
        await ctx.send(embed=create_embed(f"{E_DANGER} Fine", f"Deducted **${abs_amt:,}** from {d['username']}.", 0xff0000))

@bot.hybrid_command(name="deductsalary", aliases=["ds"], description="Owner: Deduct salary.")
async def deductsalary(ctx, duelist_identifier: str, confirm: str): # Upgraded to identifier
    if confirm.lower() != "yes": return
    d = get_duelist(duelist_identifier)
    if not d: return await ctx.send(embed=create_embed("Error", "Duelist not found.", 0xff0000))
    if not d.get('club_id'): return await ctx.send(embed=create_embed("Error", "Duelist not in a club.", 0xff0000))
    
    owner_str, owner_ids = get_club_owner_info(d['club_id'])
    if str(ctx.author.id) not in owner_ids and not ctx.author.guild_permissions.administrator: return await ctx.send(embed=create_embed("Error", "Not authorized.", 0xff0000))
    
    penalty = int(d["expected_salary"] * (DUELIST_MISS_PENALTY_PERCENT / 100))
    wallets_col.update_one({"user_id": d["user_id"]}, {"$inc": {"balance": -penalty}}, upsert=True)
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": penalty}}, upsert=True)
    log_user_activity(d["user_id"], "Penalty", f"Fined ${penalty:,} for missed match.")
    await ctx.send(embed=create_embed(f"{E_ALERT} Penalty", f"Fined **${penalty:,}** from **{d['username']}**'s wallet.", 0xff0000))
    
# ==============================================================================
#  CLUB TAX COMMANDS
# ==============================================================================

@bot.command(name="taxinfo", aliases=["ti"], description="View your club's tax status.")
async def taxinfo(ctx, *, club_name: str = None):
    query = {"owner_id": str(ctx.author.id)}
    if club_name:
        query["name"] = {"$regex": f"^{club_name}$", "$options": "i"}
        
    clubs = list(clubs_col.find(query))
    if not clubs:
        return await ctx.send(embed=create_embed("No Clubs Found", f"{E_ERROR} You don't own any clubs matching that name.", 0xff0000))
        
    embed = discord.Embed(title=f"{E_BOOK} Club Tax Information", color=0x3498db)
    if ctx.author.avatar: embed.set_thumbnail(url=ctx.author.avatar.url)
        
    for club in clubs:
        tax_amount = int(club.get("value", 0) * 0.25)
        due_date = club.get("tax_due_date")
        
        if due_date:
            time_left = due_date - datetime.now()
            days_left = max(0, time_left.days)
            status = f"⏳ {days_left} Days Left (<t:{int(due_date.timestamp())}:R>)"
        else:
            status = "✅ 30-Day Grace Period Active."
            
        embed.add_field(
            name=f"{E_CROWN} {club['name']}",
            value=f"{E_MONEY} **Tax Due:** ${tax_amount:,}\n{E_TIMER} **Deadline:** {status}\n*Tax is 25% of live worth (${club.get('value', 0):,})*",
            inline=False
        )
        
    await ctx.send(embed=embed)


@bot.command(name="paytax", aliases=["ptx"], description="Pay the tax for your club.")
async def paytax(ctx, *, club_name: str):
    club = clubs_col.find_one({"owner_id": str(ctx.author.id), "name": {"$regex": f"^{club_name}$", "$options": "i"}})
    
    if not club:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You don't own a club named **{club_name}**.", 0xff0000))
        
    tax_amount = int(club.get("value", 0) * 0.25)
    
    desc = (
        f"Are you sure you want to pay the tax for **{club['name']}**?\n\n"
        f"{E_MONEY} **Amount Due:** ${tax_amount:,}\n"
        f"{E_TIMER} **Extends Deadline By:** 30 Days"
    )
    embed = create_embed(f"{E_MONEY} Pay Club Tax", desc, 0xf1c40f)
    
    view = PayTaxView(ctx, club, tax_amount)
    await ctx.send(embed=embed, view=view)


@bot.command(name="removetax", aliases=["rtx"], description="Admin: Waive tax for a club for 1 month.")
@commands.has_permissions(administrator=True)
async def removetax(ctx, *, club_name: str):
    club = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not club or not club.get("owner_id"):
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found or has no owner.", 0xff0000))
        
    current_due = club.get("tax_due_date", datetime.now())
    new_due = max(datetime.now(), current_due) + timedelta(days=30)
    
    clubs_col.update_one({"_id": club["_id"]}, {"$set": {"tax_due_date": new_due, "tax_reminder_stage": 0}})
    
    desc = f"{E_SUCCESS} Successfully waived tax for **{club['name']}**.\n{E_TIMER} **New Deadline:** <t:{int(new_due.timestamp())}:f>"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Tax Waived", desc, 0x2ecc71))


@bot.command(name="unpaidtax", aliases=["uptx"], description="Admin: View clubs with upcoming or unpaid taxes.")
@commands.has_permissions(administrator=True)
async def unpaidtax(ctx):
    # Sort by tax due date ascending (closest to expiring first)
    clubs = list(clubs_col.find({"owner_id": {"$ne": None}, "tax_due_date": {"$ne": None}}).sort("tax_due_date", 1))
    
    if not clubs:
        return await ctx.send(embed=create_embed("All Good", f"{E_SUCCESS} No clubs have pending taxes.", 0x2ecc71))
        
    data = []
    now = datetime.now()
    
    for c in clubs:
        due = c["tax_due_date"]
        tax_amount = int(c.get("value", 0) * 0.25)
        time_left = due - now
        
        if time_left.total_seconds() < 0:
            status = "🚨 EXPIRED"
        else:
            days = time_left.days
            status = f"⏳ {days} Days Left" if days > 0 else f"⏳ {int(time_left.total_seconds() // 3600)} Hours Left"
            
        title = f"{c['name']} | Tax: ${tax_amount:,}"
        desc = f"{E_CROWN} **Owner:** <@{c['owner_id']}>\n{E_TIMER} **Due:** <t:{int(due.timestamp())}:R> ({status})"
        data.append((title, desc))
        
    view = Paginator(ctx, data, f"{E_ADMIN} Unpaid Club Taxes Queue", 0xe74c3c)
    await ctx.send(embed=view.get_embed(), view=view)

# ===========================
#   GROUP 4: ADMIN
# ===========================

@bot.command(name="forcemarket", description="Admin: Force market values to update now.")
@commands.has_permissions(administrator=True)
async def forcemarket(ctx):
    await ctx.defer() # Don't timeout
    
    updated_count = 0
    changes_log = ""
    
    # Process updates
    for c in clubs_col.find():
        current_val = c.get("value", c.get("base_price", 0))
        
        # Fluctuate
        percent_change = random.uniform(-0.03, 0.03)
        change_amount = int(current_val * percent_change)
        new_value = max(100, current_val + change_amount)
        
        clubs_col.update_one({"_id": c["_id"]}, {"$set": {"value": new_value}})
        updated_count += 1
        
        # Track first few for the log
        if updated_count <= 5:
            icon = "📈" if change_amount >= 0 else "📉"
            changes_log += f"{icon} **{c['name']}:** ${current_val:,} -> ${new_value:,}\n"

    embed = create_embed(f"{E_STARS} Market Force Updated", f"Successfully updated values for **{updated_count}** clubs.\n\n**Sample Changes:**\n{changes_log}", 0x2ecc71)
    await ctx.send(embed=embed)

# ==============================================================================
#  DM & POLL VIEW SYSTEM
# ==============================================================================

class DMPollView(discord.ui.View):
    def __init__(self, poll_id: str, options: list):
        super().__init__(timeout=None)
        self.poll_id = poll_id
        for i, opt in enumerate(options):
            # Create a button for each option
            btn = discord.ui.Button(label=opt.strip()[:80], custom_id=f"dmpoll_{poll_id}_{i}", style=discord.ButtonStyle.primary)
            btn.callback = self.make_callback(i, opt.strip())
            self.add_item(btn)

    def make_callback(self, index, option_text):
        async def callback(interaction: discord.Interaction):
            # Check if collection exists/initialize
            if db is None: return await interaction.response.send_message("Database error.", ephemeral=True)
            
            # Check if user already voted
            existing = db.dm_polls.find_one({"id": self.poll_id, "voters": str(interaction.user.id)})
            if existing:
                return await interaction.response.send_message(f"{E_ERROR} You have already voted in this poll!", ephemeral=True)
            
            # Record the vote
            db.dm_polls.update_one(
                {"id": self.poll_id},
                {"$push": {"voters": str(interaction.user.id)}, "$inc": {f"votes.{index}": 1}}
            )
            await interaction.response.send_message(f"{E_SUCCESS} Your vote for **{option_text}** has been recorded!", ephemeral=True)
        return callback

# ==============================================================================
#  ADMIN DIRECT MESSAGE COMMANDS
# ==============================================================================

@bot.hybrid_command(name="directmessage", aliases=["dm"], description="Admin: Send an official DM embed to a Role or Member.")
@commands.has_permissions(administrator=True)
async def directmessage(ctx, target: typing.Union[discord.Role, discord.Member], purpose: str, context: str, poll_options: str = None):
    """
    Usage: .directmessage @Role "The Purpose" "The Description" "Option 1, Option 2, Option 3"
    Leave poll_options blank if you don't want a poll.
    """
    await ctx.defer(ephemeral=True) # Prevent timeout if sending to a large role
    
    msg_id = f"m{get_next_id('dm_msg_id')}"
    
    # 1. Setup Poll (If options are provided)
    options = [o.strip() for o in poll_options.split(",")] if poll_options else []
    if len(options) > 5:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Maximum 5 poll options allowed.", 0xff0000), ephemeral=True)
        
    if options:
        db.dm_polls.insert_one({
            "id": msg_id,
            "purpose": purpose,
            "options": options,
            "votes": {str(i): 0 for i in range(len(options))},
            "voters": [],
            "timestamp": datetime.now()
        })
    
    # 2. Build the Premium Embed
    desc = f"{context}\n\n{E_TIMER} **Date & Time:** <t:{int(datetime.now().timestamp())}:f>\n{E_CROWN} **Sent by:** {ctx.author.display_name}"
    embed = discord.Embed(title=f"{E_ADMIN} **{purpose.upper()}**", description=desc, color=0xf1c40f)
    embed.set_footer(text=f"ID: {msg_id} • Sent by the administration of Knowle ze kingdom, if any queries ask in ask doubts channel.")
    if ctx.guild.icon:
        embed.set_thumbnail(url=ctx.guild.icon.url)
    
    # 3. Determine Targets
    success, failed = 0, 0
    targets = [target] if isinstance(target, discord.Member) else target.members
    
    if not targets:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} No members found to message.", 0xff0000), ephemeral=True)

    view = DMPollView(msg_id, options) if options else None

    # 4. Dispatch DMs
    for member in targets:
        if member.bot: continue
        try:
            await member.send(embed=embed, view=view)
            success += 1
        except discord.Forbidden:
            failed += 1 # Users who have DMs closed
            
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Dispatch Complete", f"Message `{msg_id}` processed!\n\n{E_GOLD_TICK} **Delivered:** {success}\n{E_ERROR} **Failed (DMs Closed):** {failed}", 0x2ecc71))

@bot.hybrid_command(name="checkdmpoll", aliases=["cdp"], description="Admin: Check the results of a DM poll.")
@commands.has_permissions(administrator=True)
async def checkdmpoll(ctx, poll_id: str):
    poll = db.dm_polls.find_one({"id": poll_id})
    if not poll:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Poll `{poll_id}` not found.", 0xff0000))
        
    total_votes = len(poll.get('voters', []))
    desc = f"{E_BOOK} **Purpose:** {poll['purpose']}\n{E_CHAT} **Total Votes Cast:** {total_votes}\n\n"
    
    options = poll['options']
    votes = poll.get('votes', {})
    
    for i, opt in enumerate(options):
        count = votes.get(str(i), 0)
        desc += f"{E_ARROW} **{opt}:** {count} votes\n"
        
    embed = discord.Embed(title=f"{E_ADMIN} Poll Results: {poll_id}", description=desc, color=0x3498db)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="registerclub", aliases=["rc"], description="Admin: Register club.")
@commands.has_permissions(administrator=True)
async def registerclub(ctx, name: str, base_price: HumanInt, *, slogan: str = ""):
    if clubs_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}}): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club registered.", 0xff0000))
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    cid = get_next_id("club_id")
    clubs_col.insert_one({"id": cid, "name": name, "base_price": base_price, "value": base_price, "slogan": slogan, "logo": logo_url, "total_wins": 0, "level_name": LEVEL_UP_CONFIG[0][1], "owner_id": None, "manager_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Club Registered", f"{E_ARROW} **Name:** {name}\n{E_MONEY} **Base:** ${base_price:,}\n{E_ITEMBOX} **ID:** {cid}", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="startclubauction", aliases=["sca"], description="Admin: Start club auction.")
@commands.has_permissions(administrator=True)
async def startclubauction(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    bids_col.delete_many({"item_type": "club", "item_id": c["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Auction Started", f"{E_ARROW} **Club:** {c['name']}\n{E_MONEY} **Base:** ${c['base_price']:,}", 0xe67e22, thumbnail=c.get('logo')))
    schedule_auction_timer("club", c["id"], ctx.channel.id)

@bot.hybrid_command(name="startduelistauction", aliases=["sda"], description="Admin: Start duelist auction.")
@commands.has_permissions(administrator=True)
async def startduelistauction(ctx, duelist_id: int):
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Duelist not found.", 0xff0000))
    bids_col.delete_many({"item_type": "duelist", "item_id": d["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Duelist Auction", f"{E_ARROW} **Player:** {d['username']}\n{E_MONEY} **Base:** ${d['base_price']:,}", 0x9b59b6, thumbnail=d.get('avatar_url')))
    schedule_auction_timer("duelist", d["id"], ctx.channel.id)
# bot.py Part 3 of 4 - Admin, Giveaways & Shop Backend
# ... (Continued from Part 2)

@bot.hybrid_command(name="deleteclub", aliases=["dc"], description="Admin: Delete club.")
@commands.has_permissions(administrator=True)
async def deleteclub(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    clubs_col.delete_one({"id": c['id']})
    history_col.delete_many({"club_id": c['id']})
    duelists_col.update_many({"club_id": c['id']}, {"$set": {"club_id": None, "owned_by": None}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deleted", f"Club **{club_name}** removed.", 0xff0000))

@bot.hybrid_command(name="setprefix", description="Set prefix.")
@commands.has_permissions(administrator=True)
async def setprefix(ctx, p: str):
    global cached_prefix
    config_col.update_one({"key": "prefix"}, {"$set": {"value": p}}, upsert=True)
    cached_prefix = p # Update memory immediately
    await ctx.send(f"Prefix set to: {p}")

@bot.hybrid_command(name="registerbattle", aliases=["rb"], description="Admin: Create match.")
@commands.has_permissions(administrator=True)
async def registerbattle(ctx, club_a_name: str, club_b_name: str):
    ca = clubs_col.find_one({"name": {"$regex": f"^{club_a_name}$", "$options": "i"}})
    cb = clubs_col.find_one({"name": {"$regex": f"^{club_b_name}$", "$options": "i"}})
    if not ca or not cb: return await ctx.send(embed=create_embed("Error", "Clubs not found.", 0xff0000))
    bid = get_next_id("battle_id")
    battles_col.insert_one({"id": bid, "club_a": ca['id'], "club_b": cb['id'], "status": "REGISTERED"})
    await ctx.send(embed=create_embed(f"{E_FIRE} Battle Ready", f"**{ca['name']}** vs **{cb['name']}**\nID: {bid}", 0xe74c3c))

@bot.hybrid_command(name="battleresult", aliases=["br"], description="Admin: Log match result.")
@commands.has_permissions(administrator=True)
async def battleresult(ctx, battle_id: int, winner_name: str):
    b = battles_col.find_one({"id": int(battle_id)})
    if not b: return await ctx.send(embed=create_embed("Error", "Battle not found.", 0xff0000))
    
    wc = clubs_col.find_one({"name": {"$regex": f"^{winner_name}$", "$options": "i"}})
    if not wc: return await ctx.send(embed=create_embed("Error", "Winner not found.", 0xff0000))
    
    loser_id = b['club_a'] if b['club_b'] == wc['id'] else b['club_b']
    lc = clubs_col.find_one({"id": loser_id})
    
    clubs_col.update_one({"id": wc['id']}, {"$inc": {"value": WIN_VALUE_BONUS}})
    clubs_col.update_one({"id": loser_id}, {"$inc": {"value": LOSS_VALUE_PENALTY}})
    battles_col.update_one({"id": int(battle_id)}, {"$set": {"status": "COMPLETED"}})
    update_club_level(wc['id'], 1)

    # --- DUELIST W/L MARKET UPDATE ---
    # Win: +1 Win, +1 Match, +$50k
    duelists_col.update_many(
        {"club_id": wc["_id"]}, 
        {"$inc": {"wins": 1, "matches": 1, "market_worth": 50000}}
    )
    
    # Loss: +1 Loss, +1 Match, -$50k (Minimum value $10k)
    losing_duelists = duelists_col.find({"club_id": lc["_id"]})
    for d in losing_duelists:
        new_worth = max(10000, d.get("market_worth", 100000) - 50000)
        duelists_col.update_one(
            {"_id": d["_id"]}, 
            {"$inc": {"losses": 1, "matches": 1}, "$set": {"market_worth": new_worth}}
        )
    # ---------------------------------

    banter = random.choice(BATTLE_BANTER)
    winner_emoji = resolve_emoji(random.choice(WINNER_REACTIONS))
    loser_emoji = resolve_emoji(random.choice(LOSER_REACTIONS))
    final_banter = banter.format(winner=wc['name'], loser=lc['name'], w_emoji=winner_emoji, l_emoji=loser_emoji)
    
    embed_log = create_embed(f"{E_FIRE} Match Result", f"{resolve_emoji(E_CROWN)} **Winner:** {wc['name']} {winner_emoji}\n{resolve_emoji(E_DANGER)} **Loser:** {lc['name']} {loser_emoji}\n\n_{final_banter}_", 0xe74c3c)
    await send_log("battle", embed_log)
    await ctx.send(embed=embed_log)
    
@bot.hybrid_command(name="checkclubmessages", description="Admin: Activity bonus.")
@commands.has_permissions(administrator=True)
async def checkclubmessages(ctx, club_name: str, count: int):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    bonus = (count // OWNER_MSG_COUNT_PER_BONUS) * OWNER_MSG_VALUE_BONUS
    if bonus > 0:
        clubs_col.update_one({"id": c['id']}, {"$inc": {"value": bonus}})
        await ctx.send(embed=create_embed(f"{E_BOOST} Activity Bonus", f"**{c['name']}** value increased by **${bonus:,}**.", 0x2ecc71))
    else: await ctx.send(embed=create_embed("Info", "Not enough messages.", 0x95a5a6))

@bot.hybrid_command(name="removeshares", description="Admin: Force remove shares from a user.")
@commands.has_permissions(administrator=True)
async def removeshares(ctx, group_name: str, member: discord.Member, percentage: int):
    gname = group_name.lower()
    
    # 1. Check if user is in the group
    mem_record = group_members_col.find_one({"group_name": gname, "user_id": str(member.id)})
    if not mem_record:
        return await ctx.send(embed=create_embed("Error", f"{member.mention} is not in group **{group_name}**.", 0xff0000))
    
    current_share = mem_record.get("share_percentage", 0)
    new_share = current_share - percentage
    
    action_taken = ""
    
    # 2. Logic: Reduce or Remove
    if new_share <= 0:
        # If shares drop to 0 or below, remove them from the group entirely
        group_members_col.delete_one({"_id": mem_record["_id"]})
        action_taken = f"{E_DANGER} **Removed from Group** (Shares reached 0%)"
        new_share = 0
    else:
        # Otherwise, just reduce the percentage
        group_members_col.update_one({"_id": mem_record["_id"]}, {"$set": {"share_percentage": new_share}})
        action_taken = f"{E_GOLD_TICK} **Shares Reduced**"
        
    # 3. Log and Reply
    desc = (
        f"**Group:** {group_name}\n"
        f"**User:** {member.mention}\n"
        f"**Deducted:** {percentage}%\n"
        f"**New Total:** {new_share}%\n\n"
        f"**Status:** {action_taken}"
    )
    
    await ctx.send(embed=create_embed(f"{E_ADMIN} Shares Deleted", desc, 0xe74c3c))


@bot.hybrid_command(name="adjustgroupfunds", aliases=["agf"], description="Admin: Cheat funds.")
@commands.has_permissions(administrator=True)
async def adjustgroupfunds(ctx, group_name: str, amount: HumanInt):
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": amount}})
    await ctx.send(embed=create_embed(f"{E_ADMIN} Funds Adjusted", f"Adjusted **{group_name}** by ${amount:,}.", 0xe67e22))

@bot.hybrid_command(name="auditlog", description="Owner: View logs.")
async def auditlog(ctx, lines: int = 10):
    logs = list(audit_col.find().sort("timestamp", -1).limit(lines))
    txt = "\n".join([f"[{l['timestamp'].strftime('%H:%M')}] {l['entry']}" for l in logs])
    await ctx.send(f"```{txt}```")

@bot.hybrid_command(name="resetauction", description="Owner: Clear bids.")
async def resetauction(ctx):
    bids_col.delete_many({})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Reset", "Bids cleared.", 0x2ecc71))

@bot.hybrid_command(name="transferclub", aliases=["tc"], description="Admin: Transfer club.")
@commands.has_permissions(administrator=True)
async def transferclub(ctx, old_grp: str, new_grp: str):
    c = clubs_col.find_one({"owner_id": f"group:{old_grp.lower()}"})
    if c:
        clubs_col.update_one({"id": c['id']}, {"$set": {"owner_id": f"group:{new_grp.lower()}"}})
        await ctx.send(embed=create_embed(f"{E_ADMIN} Transferred", f"Club transferred to {new_grp}.", 0xe67e22))
    else: await ctx.send(embed=create_embed("Error", "No club found.", 0xff0000))

@bot.hybrid_command(name="tip", aliases=["tp"], description="Admin: Add money to user.")
@commands.has_permissions(administrator=True)
async def tip(ctx, member: discord.Member, amount: HumanInt):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Received tip of ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Tip", f"Added **${amount:,}** to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="deduct_user", aliases=["du"], description="Admin: Deduct money.")
@commands.has_permissions(administrator=True)
async def deduct_user(ctx, member: discord.Member, amount: HumanInt):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Deducted ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Deduct", f"Removed **${amount:,}** from {member.mention}.", 0xff0000))

# ==============================================================================
#  MASS CURRENCY ADMIN COMMANDS
# ==============================================================================

def get_user_list_string(members):
    """Helper to format user lists nicely without hitting Discord's embed character limits."""
    user_list = ", ".join([m.display_name for m in members])
    return user_list[:120] + "..." if len(user_list) > 120 else user_list

@bot.hybrid_command(name="masstip", aliases=["mtip"], description="Admin: Add cash to multiple users.")
@commands.has_permissions(administrator=True)
async def masstip(ctx, amount: HumanInt, members: commands.Greedy[discord.Member]):
    if not members or amount <= 0: 
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Usage: `.masstip <amount> @user1 @user2`", 0xff0000), ephemeral=True)
    
    for m in members: 
        wallets_col.update_one({"user_id": str(m.id)}, {"$inc": {"balance": amount}}, upsert=True)
    
    desc = f"{E_SUCCESS} Added {E_MONEY} **${amount:,}** to **{len(members)}** users.\n\n**Users:** {get_user_list_string(members)}"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Mass Tip", desc, 0x2ecc71))

@bot.hybrid_command(name="massdeduct", aliases=["mdeduct"], description="Admin: Remove cash from multiple users.")
@commands.has_permissions(administrator=True)
async def massdeduct(ctx, amount: HumanInt, members: commands.Greedy[discord.Member]):
    if not members or amount <= 0: 
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Usage: `.massdeduct <amount> @user1 @user2`", 0xff0000), ephemeral=True)
    
    for m in members: 
        wallets_col.update_one({"user_id": str(m.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    
    desc = f"{E_SUCCESS} Removed {E_MONEY} **${amount:,}** from **{len(members)}** users.\n\n**Users:** {get_user_list_string(members)}"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Mass Deduct", desc, 0xe74c3c))

@bot.hybrid_command(name="massaddpc", aliases=["mapc"], description="Admin: Add PC to multiple users.")
@commands.has_permissions(administrator=True)
async def massaddpc(ctx, amount: int, members: commands.Greedy[discord.Member]):
    if not members or amount <= 0: 
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Usage: `.massaddpc <amount> @user1 @user2`", 0xff0000), ephemeral=True)
    
    for m in members: 
        wallets_col.update_one({"user_id": str(m.id)}, {"$inc": {"pc": amount}}, upsert=True)
    
    desc = f"{E_SUCCESS} Added {E_PC} **{amount:,}** to **{len(members)}** users.\n\n**Users:** {get_user_list_string(members)}"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Mass Add PC", desc, 0x2ecc71))

@bot.command(name="massbox", aliases=["mbox"], description="Admin: Give PC Boxes to multiple users.")
@commands.has_permissions(administrator=True)
async def massbox(ctx, amount: int, members: commands.Greedy[discord.Member]):
    # 1. Error Catching
    if amount <= 0:
        return await ctx.send(embed=create_embed("Invalid Amount", f"{E_ERROR} You must give at least 1 PC Box.", 0xff0000), ephemeral=True)
    if not members:
        return await ctx.send(embed=create_embed("No Users Mentioned", f"{E_ERROR} You must mention at least one user.\n**Usage:** `.mbox <amount> @user1 @user2`", 0xff0000), ephemeral=True)

    success_count = 0
    
    # 2. Process Each Mentioned User
    for member in members:
        if member.bot: continue # Skip bots
        
        # Add the box(es) to their database profile
        wallets_col.update_one(
            {"user_id": str(member.id)},
            {"$inc": {"pc_boxes": amount}},
            upsert=True
        )
        success_count += 1
        
        # 3. Send the Premium DM
        try:
            dm_desc = (
                f"You have been awarded **{amount:,}x** {E_ITEMBOX} **PC Box(es)** by the Administration!\n\n"
                f"Type `.ob` or `/openbox` in the server to crack them open and claim your PC."
            )
            embed = create_embed(f"{E_GIVEAWAY} Special Reward!", dm_desc, 0x2ecc71)
            await member.send(embed=embed)
        except:
            pass # Ignore if the user has their DMs locked
            
    # 4. Send the Admin Confirmation in Chat
    desc = f"{E_SUCCESS} Successfully added **{amount:,}** {E_ITEMBOX} PC Box(es) to **{success_count}** user(s)!"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Mass Box Transfer", desc, 0x3498db))    

# ==========================================================
# 🛑 ECONOMY MASTER SWITCHES
# ==========================================================
@bot.command(name="stopbotp2")
@commands.has_permissions(administrator=True)
async def stopbotp2(ctx):
    bot.p2_economy_open = False # Flips the master switch OFF
    
    desc = (
        f"{E_ALERT} **PokéTwo Economy Services Suspended**\n\n"
        f"All PC features have been safely locked down. The bot will now completely ignore the following systems:\n"
        f"▫️ Wallets & Profiles\n"
        f"▫️ The `.shop` & PC Boxes\n"
        f"▫️ PC Deposits & Withdrawals\n"
        f"▫️ Login Rewards (`.lr`)\n\n"
        f"*(Clubs, Duelists, Auctions, and Giveaways are still fully operational.)*"
    )
    await ctx.send(embed=create_embed("Economy Offline", desc, 0xff0000))

@bot.command(name="openbotp2")
@commands.has_permissions(administrator=True)
async def openbotp2(ctx):
    bot.p2_economy_open = True # Flips the master switch ON
    
    desc = f"{E_SUCCESS} **PokéTwo Economy Services Restored**\n\nAll PC, Shop, Wallet, and Deposit features are back online and accepting user commands."
    await ctx.send(embed=create_embed("Economy Online", desc, 0x2ecc71))

# ==============================================================================
#  TROPHY AWARD COMMANDS (ADMIN ONLY)
# ==============================================================================

@bot.command(name="ucl", description="Admin: Award a UCL trophy to a club.")
@commands.has_permissions(administrator=True)
async def ucl(ctx, *, club_name: str):
    club = clubs_col.find_one_and_update(
        {"name": {"$regex": f"^{club_name}$", "$options": "i"}},
        {"$inc": {"t_ucl": 1}},
        return_document=ReturnDocument.AFTER
    )
    if not club: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    await ctx.send(embed=create_embed(f"{E_UCL} UCL Awarded!", f"{E_SUCCESS} **{club['name']}** won the UCL!\nThey now have **{club.get('t_ucl', 1)}x** {E_UCL}", 0xf1c40f))

@bot.command(name="league", description="Admin: Award a League title to a club.")
@commands.has_permissions(administrator=True)
async def league(ctx, *, club_name: str):
    club = clubs_col.find_one_and_update(
        {"name": {"$regex": f"^{club_name}$", "$options": "i"}},
        {"$inc": {"t_league": 1}},
        return_document=ReturnDocument.AFTER
    )
    if not club: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    await ctx.send(embed=create_embed(f"{E_LEAGUE} League Awarded!", f"{E_SUCCESS} **{club['name']}** won the League!\nThey now have **{club.get('t_league', 1)}x** {E_LEAGUE}", 0xf1c40f))

@bot.command(name="supercup", description="Admin: Award a Super Cup to a club.")
@commands.has_permissions(administrator=True)
async def supercup(ctx, *, club_name: str):
    club = clubs_col.find_one_and_update(
        {"name": {"$regex": f"^{club_name}$", "$options": "i"}},
        {"$inc": {"t_supercup": 1}},
        return_document=ReturnDocument.AFTER
    )
    if not club: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    await ctx.send(embed=create_embed(f"{E_SUPERCUP} Super Cup Awarded!", f"{E_SUCCESS} **{club['name']}** won the Super Cup!\nThey now have **{club.get('t_supercup', 1)}x** {E_SUPERCUP}", 0xf1c40f))

@bot.command(name="ballondor", description="Admin: Award a Ballon d'Or to a user.")
@commands.has_permissions(administrator=True)
async def ballondor(ctx, user: discord.Member):
    w = wallets_col.find_one_and_update({"user_id": str(user.id)}, {"$inc": {"t_ballondor": 1}}, upsert=True, return_document=ReturnDocument.AFTER)
    await ctx.send(embed=create_embed(f"{E_BALLONDOR} Ballon d'Or Awarded!", f"{E_SUCCESS} <@{user.id}> won the Ballon d'Or!\nThey now have **{w.get('t_ballondor', 1)}x** {E_BALLONDOR}", 0xf1c40f))

@bot.command(name="superballondor", aliases=["sballondor"], description="Admin: Award a Super Ballon d'Or to a user.")
@commands.has_permissions(administrator=True)
async def superballondor(ctx, user: discord.Member):
    w = wallets_col.find_one_and_update({"user_id": str(user.id)}, {"$inc": {"t_sballondor": 1}}, upsert=True, return_document=ReturnDocument.AFTER)
    await ctx.send(embed=create_embed(f"{E_SUPERBALLONDOR} Super Ballon d'Or Awarded!", f"{E_SUCCESS} <@{user.id}> won the Super Ballon d'Or!\nThey now have **{w.get('t_sballondor', 1)}x** {E_SUPERBALLONDOR}", 0xf1c40f))

@bot.hybrid_command(name="massaddsc", aliases=["masc"], description="Admin: Add Shiny Coins to multiple users.")
@commands.has_permissions(administrator=True)
async def massaddsc(ctx, amount: int, members: commands.Greedy[discord.Member]):
    if not members or amount <= 0: 
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Usage: `.massaddsc <amount> @user1 @user2`", 0xff0000), ephemeral=True)
    
    for m in members: 
        wallets_col.update_one({"user_id": str(m.id)}, {"$inc": {"shiny_coins": amount}}, upsert=True)
    
    desc = f"{E_SUCCESS} Added {E_SHINY} **{amount:,}** to **{len(members)}** users.\n\n**Users:** {get_user_list_string(members)}"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Mass Add Shiny", desc, 0x2ecc71))

@bot.hybrid_command(name="massremovepc", aliases=["mrpc"], description="Admin: Remove PC from multiple users.")
@commands.has_permissions(administrator=True)
async def massremovepc(ctx, amount: int, members: commands.Greedy[discord.Member]):
    if not members or amount <= 0: 
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Usage: `.massremovepc <amount> @user1 @user2`", 0xff0000), ephemeral=True)
    
    for m in members: 
        wallets_col.update_one({"user_id": str(m.id)}, {"$inc": {"pc": -amount}}, upsert=True)
    
    desc = f"{E_SUCCESS} Removed {E_PC} **{amount:,}** from **{len(members)}** users.\n\n**Users:** {get_user_list_string(members)}"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Mass Remove PC", desc, 0xe74c3c))

@bot.hybrid_command(name="massremovesc", aliases=["mrsc"], description="Admin: Remove Shiny Coins from multiple users.")
@commands.has_permissions(administrator=True)
async def massremovesc(ctx, amount: int, members: commands.Greedy[discord.Member]):
    if not members or amount <= 0: 
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Usage: `.massremovesc <amount> @user1 @user2`", 0xff0000), ephemeral=True)
    
    for m in members: 
        wallets_col.update_one({"user_id": str(m.id)}, {"$inc": {"shiny_coins": -amount}}, upsert=True)
    
    desc = f"{E_SUCCESS} Removed {E_SHINY} **{amount:,}** from **{len(members)}** users.\n\n**Users:** {get_user_list_string(members)}"
    await ctx.send(embed=create_embed(f"{E_ADMIN} Mass Remove Shiny", desc, 0xe74c3c))

@bot.hybrid_command(name="setclubmanager", aliases=["scm"], description="Admin: Set manager.")
@commands.has_permissions(administrator=True)
async def setclubmanager(ctx, club_name: str, member: discord.Member):
    clubs_col.update_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}}, {"$set": {"manager_id": str(member.id)}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Manager Set", f"{member.mention} is now manager of {club_name}.", 0x2ecc71))

@bot.hybrid_command(name="logpayment", aliases=["lp"], description="Admin: Log payment.")
@commands.has_permissions(administrator=True)
async def logpayment(ctx, user: discord.Member, amount: HumanInt, *, reason: str):
    embed = create_embed(f"{E_ADMIN} Payment Log", f"**From:** {ctx.author.mention}\n**To:** {user.mention}\n**Amount:** ${amount:,}\n**Reason:** {reason}\n**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}", 0x9b59b6)
    await send_log("withdraw", embed)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Logged", "Logged.", 0x2ecc71))

@bot.command(name="getpc", aliases=["gpc"], description="Start a PC withdrawal process.")
async def getpc(ctx):
    desc = f"{E_PC} Click the button below to open the withdrawal form.\nEnsure you have enough PC in your balance before submitting!"
    await ctx.send(embed=create_embed(f"{E_MONEY} Withdraw PC", desc, 0x3498db), view=PCWithdrawView())

@bot.command(name="claimstatus", aliases=["cs"], description="Check the status and queue of your PC claim.")
async def claimstatus(ctx):
    claims = list(db.pc_claims.find({"user_id": str(ctx.author.id), "status": "PENDING"}).sort("created_at", 1))
    if not claims:
        return await ctx.send(embed=create_embed("No Active Claims", f"{E_ERROR} You have no pending PC withdrawals.", 0x95a5a6))
        
    embed = discord.Embed(title=f"{E_TIMER} Your Pending Claims", color=0x3498db)
    for c in claims:
        # Calculate Queue position (How many pending claims exist before this one)
        queue_pos = db.pc_claims.count_documents({"status": "PENDING", "created_at": {"$lt": c["created_at"]}}) + 1
        
        status_text = "⏳ Waiting for Timer" if datetime.now() < c["unlocks_at"] else "✅ Ready for Admin Approval"
        
        embed.add_field(
            name=f"Claim `{c['id']}`", 
            value=f"{E_PC} **Amount:** {c['amount']:,}\n{E_ITEMBOX} **Queue Pos:** #{queue_pos}\n{E_TIMER} **Unlocks:** <t:{int(c['unlocks_at'].timestamp())}:R>\n**Status:** {status_text}", 
            inline=False
        )
    await ctx.send(embed=embed)

@bot.command(name="claiminfo", aliases=["csinfo"], description="Admin: View details of a specific claim.")
@commands.has_permissions(administrator=True)
async def claiminfo(ctx, claim_id: str):
    c = db.pc_claims.find_one({"id": claim_id.lower()})
    if not c:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Claim `{claim_id}` not found.", 0xff0000))
        
    desc = (
        f"{E_CROWN} **User:** <@{c['user_id']}>\n"
        f"{E_PC} **Amount:** {c['amount']:,}\n"
        f"{E_ITEMBOX} **Market ID:** {c['market_id']}\n"
        f"{E_ADMIN} **Status:** {c['status']}\n\n"
        f"{E_TIMER} **Created:** <t:{int(c['created_at'].timestamp())}:f>\n"
        f"{E_TIMER} **Unlocks At:** <t:{int(c['unlocks_at'].timestamp())}:f>"
    )
    await ctx.send(embed=create_embed(f"{E_BOOK} Claim Info: {c['id']}", desc, 0x9b59b6))

@bot.command(name="claimapproved", aliases=["ca"], description="Admin: Approve a PC withdrawal claim.")
@commands.has_permissions(administrator=True)
async def claimapproved(ctx, claim_id: str):
    c = db.pc_claims.find_one({"id": claim_id.lower()})
    if not c or c['status'] != "PENDING":
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Claim `{claim_id}` not found or already processed.", 0xff0000))
        
    # Check if user still has the funds
    w = get_wallet(c['user_id'])
    if w.get("pc", 0) < c['amount']:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} User no longer has enough PC. Use `.cr` to reject.", 0xff0000))
        
    # Deduct PC and update DB
    wallets_col.update_one({"user_id": c['user_id']}, {"$inc": {"pc": -c['amount']}})
    db.pc_claims.update_one({"_id": c["_id"]}, {"$set": {"status": "APPROVED", "processed_at": datetime.now()}})
    
    # Send Premium DM to user
    try:
        user = await bot.fetch_user(int(c['user_id']))
        dm_desc = f"Your PC withdrawal request for **{c['amount']:,}** {E_PC} (Market ID: `{c['market_id']}`) has been officially processed and approved by the administration!"
        await user.send(embed=create_embed(f"{E_SUCCESS} Claim Approved", dm_desc, 0x2ecc71))
    except: pass
    
    # Auto-Log to Withdraw Channel
    log_ch = bot.get_channel(LOG_CHANNELS["withdraw"])
    if log_ch:
        time_taken = str(datetime.now() - c['created_at']).split('.')[0] # Formats cleanly
        log_desc = (
            f"**Claim ID:** `{c['id']}`\n"
            f"**User:** <@{c['user_id']}>\n"
            f"**Admin:** {ctx.author.mention}\n"
            f"**Amount:** {c['amount']:,} {E_PC}\n"
            f"**Market ID:** {c['market_id']}\n"
            f"**Requested On:** <t:{int(c['created_at'].timestamp())}:f>\n"
            f"**Processing Time:** {time_taken}"
        )
        await log_ch.send(embed=create_embed(f"{E_MONEY} Withdrawal Approved", log_desc, 0x2ecc71))
        
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Claim Approved", f"Claim `{c['id']}` has been finalized and PC has been deducted.", 0x2ecc71))

@bot.command(name="claimrejected", aliases=["cr"], description="Admin: Reject a PC withdrawal claim.")
@commands.has_permissions(administrator=True)
async def claimrejected(ctx, claim_id: str, *, reason: str = "No reason provided."):
    c = db.pc_claims.find_one({"id": claim_id.lower()})
    if not c or c['status'] != "PENDING":
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Claim `{claim_id}` not found or already processed.", 0xff0000))
        
    db.pc_claims.update_one({"_id": c["_id"]}, {"$set": {"status": "REJECTED", "processed_at": datetime.now()}})
    
    # Send Premium DM to user
    try:
        user = await bot.fetch_user(int(c['user_id']))
        dm_desc = f"Your PC withdrawal request for **{c['amount']:,}** {E_PC} (Market ID: `{c['market_id']}`) has been rejected.\n\n**Reason:** {reason}"
        await user.send(embed=create_embed(f"{E_DANGER} Claim Rejected", dm_desc, 0xff0000))
    except: pass
        
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Claim Rejected", f"Claim `{c['id']}` has been rejected.", 0xff0000))

@bot.command(name="pendingclaims", aliases=["pendingpc", "pclist"], description="Admin: View all pending PC withdrawal claims.")
@commands.has_permissions(administrator=True)
async def pendingclaims(ctx):
    # Fetch all pending claims, sorting the oldest ones to the top of the queue
    claims = list(db.pc_claims.find({"status": "PENDING"}).sort("created_at", 1))
    
    if not claims:
        return await ctx.send(embed=create_embed("Queue Clear!", f"{E_SUCCESS} There are currently no pending PC claims.", 0x2ecc71))
        
    data = []
    for i, c in enumerate(claims):
        # Determine if the timer is still ticking or if it's ready for approval
        status_text = "⏳ Waiting" if datetime.now() < c["unlocks_at"] else "✅ READY"
        
        title = f"#{i+1} | Claim ID: {c['id']}"
        desc = (
            f"{E_CROWN} **User:** <@{c['user_id']}>\n"
            f"{E_PC} **Amount:** {c['amount']:,}\n"
            f"{E_ITEMBOX} **Market ID:** `{c['market_id']}`\n"
            f"{E_TIMER} **Unlocks:** <t:{int(c['unlocks_at'].timestamp())}:R> ({status_text})"
        )
        data.append((title, desc))
        
    view = Paginator(ctx, data, f"{E_ADMIN} Pending PC Claims Queue", 0xf1c40f)
    await ctx.send(embed=view.get_embed(), view=view)


@bot.command(name="claimhistory", aliases=["chistory"], description="Admin: View recently processed PC claims.")
@commands.has_permissions(administrator=True)
async def claimhistory(ctx):
    # Fetch the 30 most recently processed (approved/rejected) claims
    claims = list(db.pc_claims.find({"status": {"$ne": "PENDING"}}).sort("processed_at", -1).limit(30))
    
    if not claims:
        return await ctx.send(embed=create_embed("Empty History", f"{E_ERROR} No processed claims found in the database.", 0x95a5a6))
        
    data = []
    for c in claims:
        icon = E_SUCCESS if c['status'] == "APPROVED" else E_DANGER
        title = f"Claim ID: {c['id']} | {c['status']}"
        desc = (
            f"{icon} **User:** <@{c['user_id']}>\n"
            f"{E_PC} **Amount:** {c['amount']:,}\n"
            f"{E_TIMER} **Processed:** <t:{int(c.get('processed_at', c['created_at']).timestamp())}:d>"
        )
        data.append((title, desc))
        
    view = Paginator(ctx, data, f"{E_BOOK} Recent PC Claims History", 0x3498db)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="admin_reset_all", description="Owner: Reset EVERYTHING.")
@commands.has_permissions(administrator=True)
async def admin_reset_all(ctx):
    if BOT_OWNER_ID and ctx.author.id != BOT_OWNER_ID: return
    await ctx.send(embed=create_embed(f"{E_DANGER} WARNING", "Resetting EVERYTHING...", 0xff0000))
    clubs_col.update_many({}, {"$set": {"total_wins": 0, "level_name": LEVEL_UP_CONFIG[0][1], "owner_id": None, "value": 1000000}})
    battles_col.delete_many({})
    history_col.delete_many({})
    profiles_col.update_many({}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
    duelists_col.update_many({}, {"$set": {"owned_by": None, "club_id": None}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Reset", "System Reset Complete.", 0x2ecc71))

@bot.hybrid_command(name="forcewinner", aliases=["fw"], description="Owner: Force win.")
@commands.has_permissions(administrator=True)
async def forcewinner(ctx, item_type: str, item_id: int, winner_str: str, amount: HumanInt):
    bids_col.insert_one({"bidder": winner_str, "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    await finalize_auction(item_type, int(item_id), ctx.channel.id)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Force Win", f"Forced winner **{winner_str}**.", 0xe67e22))

@bot.command(name="freezeauction", aliases=["fa"], description="Owner: Freeze auctions.")
@commands.has_permissions(administrator=True)
async def freezeauction(ctx):
    global bidding_frozen
    bidding_frozen = True
    await ctx.send(embed=create_embed(f"{E_DANGER} Frozen", "Auctions frozen.", 0xff0000))

@bot.command(name="unfreezeauction", aliases=["ufa"], description="Owner: Resume auctions.")
@commands.has_permissions(administrator=True)
async def unfreezeauction(ctx):
    global bidding_frozen
    bidding_frozen = False
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Unfrozen", "Auctions resumed.", 0x2ecc71))



@bot.hybrid_command(name="checkdeals", aliases=["cd"], description="Admin: View pending club deals.")
@commands.has_permissions(administrator=True)
async def checkdeals(ctx):
    # This command is retained for legacy Club buying support. New Shop Approvals are separate.
    deals = list(pending_deals_col.find().sort("timestamp", 1))
    if not deals: return await ctx.send(embed=create_embed(f"{E_SUCCESS} All Clear", "No pending deals.", 0x2ecc71))
    data = []
    for d in deals:
        buyer_display = d['buyer_id'].replace("group:", "Group: ").title() if "group:" in d['buyer_id'] else f"<@{d['buyer_id']}>"
        data.append((f"Deal #{d['id']} | {d['club_name']}", f"{E_MONEY} **Price:** ${d['price']:,}\n{E_CROWN} **Buyer:** {buyer_display}"))
    view = Paginator(ctx, data, f"{E_ADMIN} Pending Club Approvals", 0xe67e22, 5)
    await ctx.send(embed=view.get_embed(), view=view)
@bot.hybrid_command(name="managedeal", aliases=["md"], description="Admin: Approve or Reject a deal.")
@commands.has_permissions(administrator=True)
async def managedeal(ctx, deal_id: str, action: str):
    action = action.lower()
    if action not in ["approve", "reject"]: 
        return await ctx.send(embed=create_embed("Error", "Action must be `approve` or `reject`.", 0xff0000))
    
    # 1. SEARCH LOGIC (Integer first, then String)
    deal = None
    if deal_id.isdigit():
        deal = db.pending_deals.find_one({"id": int(deal_id)})
    if not deal:
        deal = db.pending_deals.find_one({"id": str(deal_id)})

    # 2. NOT FOUND HANDLER
    if not deal: 
        all_deals = list(db.pending_deals.find({}))
        available_ids = [str(d.get('id')) for d in all_deals]
        desc = f"❌ Deal ID `{deal_id}` not found.\n**Available IDs:** {', '.join(available_ids) if available_ids else 'None'}"
        return await ctx.send(embed=create_embed("Deal Not Found", desc, 0xff0000))
    
    # 3. UNIVERSAL CLUB DEAL LOGIC
    # We check if the type is ANY of the valid club buy types
    if deal.get("type") in ["club_buy", "user", "group"]:
        c = clubs_col.find_one({"id": deal['club_id']})
        if not c: return await ctx.send(embed=create_embed("Error", "Club referenced in deal not found.", 0xff0000))

        buyer_id = deal['buyer_id']
        price = int(deal['price'])
        
        # --- REJECTION LOGIC ---
        if action == "reject":
            if buyer_id.startswith("group:") or deal.get("type") == "group":
                # Handle Group Refund
                gname = buyer_id.replace("group:", "") if "group:" in buyer_id else deal.get('buyer_id').replace("group:", "")
                groups_col.update_one({"name": gname}, {"$inc": {"funds": price}})
            else:
                # Handle User Refund
                wallets_col.update_one({"user_id": buyer_id}, {"$inc": {"balance": price}})
                try: 
                    user = await bot.fetch_user(int(buyer_id))
                    await user.send(embed=create_embed(f"{E_DANGER} Deal Rejected", f"Your request to buy **{deal['club_name']}** was rejected.\n{E_MONEY} **${price:,}** refunded.", 0xff0000))
                except: pass
            
            db.pending_deals.delete_one({"_id": deal["_id"]})
            await ctx.send(embed=create_embed(f"{E_SUCCESS} Rejected", f"Deal #{deal_id} rejected. Funds refunded.", 0x2ecc71))
            return

        # --- APPROVAL LOGIC ---
        if action == "approve":
            if c.get("owner_id"):
                # Already Owned -> Refund
                if buyer_id.startswith("group:") or deal.get("type") == "group":
                    gname = buyer_id.replace("group:", "")
                    groups_col.update_one({"name": gname}, {"$inc": {"funds": price}})
                else:
                    wallets_col.update_one({"user_id": buyer_id}, {"$inc": {"balance": price}})
                
                db.pending_deals.delete_one({"_id": deal["_id"]})
                return await ctx.send(embed=create_embed("Error", "Club is already owned! Deal cancelled and refunded.", 0xff0000))

           # Transfer Ownership with Tax Timer
            clubs_col.update_one(
                {"id": c["id"]}, 
                {"$set": {
                    "owner_id": buyer_id,
                    "tax_due_date": datetime.now() + timedelta(days=30),
                    "tax_reminder_stage": 0
                }}
            )
            
            # If User (not group), update profile
            if not buyer_id.startswith("group:") and deal.get("type") != "group":
                profiles_col.update_one({"user_id": buyer_id}, {"$set": {"owned_club_id": c["id"], "owned_club_share": 100}}, upsert=True)
                log_user_activity(buyer_id, "Purchase", f"Bought {c['name']} (Approved)")
            
            # Log
            history_col.insert_one({"club_id": c["id"], "winner": buyer_id, "amount": price, "timestamp": datetime.now(), "type": "market_buy"})
            
            display_owner = buyer_id
            if "group:" in buyer_id: display_owner = f"Group: {buyer_id.replace('group:', '').title()}"
            else: display_owner = f"<@{buyer_id}>"

            log_embed = create_embed(f"{E_GIVEAWAY} CLUB SOLD", f"Transfer Approved by {ctx.author.mention}\n\n{E_STAR} **Club:** {c['name']}\n{E_CROWN} **New Owner:** {display_owner}\n{E_MONEY} **Price:** ${price:,}", 0xf1c40f)
            if c.get("logo"): log_embed.set_thumbnail(url=c['logo'])
            await send_log("club", log_embed)
            
            db.pending_deals.delete_one({"_id": deal["_id"]})
            await ctx.send(embed=create_embed(f"{E_SUCCESS} Approved", f"Deal #{deal_id} approved. Ownership transferred.", 0x2ecc71))
            return

    # 4. UNKNOWN DEAL TYPE HANDLER
    await ctx.send(embed=create_embed("Error", f"Unknown deal type: `{deal.get('type')}`\nID: {deal_id}", 0xff0000))

# ==============================================================================
#  NEW ADMIN COMMANDS: REMOVALS & HISTORY
# ==============================================================================

@bot.group(name="remove", description="Admin: Remove assets from users.")
@commands.has_permissions(administrator=True)
async def remove(ctx):
    if ctx.invoked_subcommand is None:
        await ctx.send(embed=create_embed("Error", f"Usage:\n`.remove sh <amt> @User`\n`.remove pc <amt> @User`\n`.remove inv <Item> @User`", 0xff0000))

@remove.command(name="sh", description="Remove Shiny Coins.")
async def remove_sh(ctx, amount: int, member: discord.Member):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"shiny_coins": -amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Removed SC", f"Removed **{amount:,}** {E_SHINY} from {member.mention}.", 0xe74c3c))

@remove.command(name="pc", description="Remove PokeCoins.")
async def remove_pc(ctx, amount: int, member: discord.Member):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"pc": -amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Removed PC", f"Removed **{amount:,}** {E_PC} from {member.mention}.", 0xe74c3c))

@remove.command(name="inv", description="Remove Item/Pokemon from user.")
async def remove_inv(ctx, member: discord.Member, *, item_name: str):
    # Smart Search: Find item using partial match (case insensitive)
    # e.g., "pika" matches "Level 50 Pikachu"
    item = inventory_col.find_one({
        "user_id": str(member.id), 
        "name": {"$regex": re.escape(item_name), "$options": "i"}
    })
    
    if not item: 
        return await ctx.send(embed=create_embed("Error", f"{member.display_name} does not have any item matching **{item_name}**.", 0xff0000))
    
    # Remove 1 quantity
    inventory_col.update_one({"_id": item["_id"]}, {"$inc": {"quantity": -1}})
    
    # Cleanup: If quantity reaches 0, delete the item from DB
    updated_item = inventory_col.find_one({"_id": item["_id"]})
    if updated_item and updated_item["quantity"] <= 0:
        inventory_col.delete_one({"_id": item["_id"]})
    
    await ctx.send(embed=create_embed(f"{E_DANGER} Removed Item", f"Removed 1x **{item['name']}** from {member.mention}.", 0xff0000))

@bot.hybrid_command(name="resetinv", aliases=["ri"], description="Admin: Clear a user's entire inventory.")
@commands.has_permissions(administrator=True)
async def resetinv(ctx, member: discord.Member):
    # Clears all items associated with this user ID
    result = inventory_col.delete_many({"user_id": str(member.id)})
    
    desc = (
        f"{E_ADMIN} **Action:** Full Inventory Wipe\n"
        f"{E_CROWN} **Target User:** {member.mention}\n"
        f"{E_DANGER} **Items Deleted:** {result.deleted_count}"
    )
    
    embed = create_embed(f"{E_DANGER} Inventory Reset", desc, 0xff0000)
    if member.avatar: 
        embed.set_thumbnail(url=member.avatar.url)
        
    await ctx.send(embed=embed)

@bot.hybrid_command(name="removeinventory", aliases=["rminv"], description="Admin: Delete a specific item from a user's inventory.")
@commands.has_permissions(administrator=True)
async def removeinventory(ctx, member: discord.Member, *, item_name: str):
    # Smart search: Check both 'item_name' and 'name' fields (case-insensitive)
    query_regex = {"$regex": re.escape(item_name), "$options": "i"}
    item = inventory_col.find_one({
        "user_id": str(member.id),
        "$or": [{"item_name": query_regex}, {"name": query_regex}]
    })
    
    if not item:
        return await ctx.send(embed=create_embed("Item Not Found", f"{E_ERROR} {member.display_name} does not own any item matching **{item_name}**.", 0xff0000), ephemeral=True)
    
    # Get exact name from database record
    found_name = item.get("item_name") or item.get("name", "Unknown Item")
    
    # Completely delete this specific item instance from the database
    inventory_col.delete_one({"_id": item["_id"]})
    
    desc = (
        f"{E_ADMIN} **Action:** Item Confiscation\n"
        f"{E_CROWN} **Target User:** {member.mention}\n"
        f"{E_ITEMBOX} **Item Removed:** {found_name}"
    )
    
    embed = create_embed(f"{E_SUCCESS} Item Removed", desc, 0xe74c3c)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="tradehistory", aliases=["th"], description="View a user's trade history.")
async def tradehistory(ctx, user: discord.Member = None):
    target = user or ctx.author
    uid = str(target.id)
    
    # Need to initialize this collection in Part 1 if not done: trade_history_col = db.trade_history
    trades = list(db.trade_history.find({"users": uid}).sort("timestamp", -1).limit(10))
    
    if not trades: 
        return await ctx.send(embed=create_embed(f"{E_BOOK} History", f"No trade history found for {target.display_name}.", 0x95a5a6))
    
    data = []
    for t in trades:
        u1, u2 = t['users']
        other_id = u2 if u1 == uid else u1
        ts = t['timestamp'].strftime('%Y-%m-%d')
        
        # Format the deal summary
        details = f"**Partner:** <@{other_id}>\n"
        details += f"**Received:** {t['offers'][other_id]}\n"
        details += f"**Given:** {t['offers'][uid]}"
        
        data.append((f"{E_AUCTION} Trade on {ts}", details))
        
    view = Paginator(ctx, data, f"{E_BOOK} Trade History: {target.display_name}", 0x3498db)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.command(name="servertradehistory", aliases=["sth"], description="View global server trade logs.")
async def servertradehistory(ctx):
    trades = list(db.trade_history.find().sort("timestamp", -1).limit(15))
    
    if not trades: return await ctx.send(embed=create_embed("Server History", "No trades recorded yet.", 0x95a5a6))
    
    data = []
    for t in trades:
        u1, u2 = t['users']
        ts = t['timestamp'].strftime('%Y-%m-%d %H:%M')
        
        details = f"<@{u1}> ↔️ <@{u2}>\n"
        details += f"**{t['summary']}**" # We will save a short summary string
        
        data.append((f"{E_AUCTION} Trade | {ts}", details))
        
    view = Paginator(ctx, data, f"{E_STARS} Server Trade Logs", 0xf1c40f)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="syncduelists", description="Admin: Upgrade old duelists to the new esports engine.")
@commands.has_permissions(administrator=True)
async def syncduelists(ctx):
    old_duelists = duelists_col.find({})
    count = 0
    
    for d in old_duelists:
        updates = {}
        # Inject the missing esports keys if they don't have them
        if "user_id" not in d: updates["user_id"] = d.get("discord_user_id")
        if "duelist_id" not in d: updates["duelist_id"] = f"D{d.get('id')}"
        if "market_worth" not in d: updates["market_worth"] = d.get("base_price", 100000)
        if "status" not in d: updates["status"] = "Signed" if d.get("club_id") else "Free Agent"
        if "transfer_listed" not in d: updates["transfer_listed"] = False
        if "wins" not in d: updates["wins"] = 0
        if "losses" not in d: updates["losses"] = 0
        if "draws" not in d: updates["draws"] = 0
        if "matches" not in d: updates["matches"] = 0
        if "mvps" not in d: updates["mvps"] = 0

        if updates:
            duelists_col.update_one({"_id": d["_id"]}, {"$set": updates})
            count += 1
            
    await ctx.send(embed=create_embed("Sync Complete", f"{E_SUCCESS} Successfully upgraded **{count}** legacy duelists to the new Esports Engine!", 0x2ecc71))
    
# ===========================
#   GROUP 5: GIVEAWAYS
# ===========================

async def run_giveaway(ctx, prize, winners_count, duration_seconds, description, required_role_ids=None, weighted=False, image_url=None):
    end_time = int(datetime.now().timestamp() + duration_seconds)
    embed = discord.Embed(title=f"{E_GIVEAWAY} {prize}", description=description, color=0xe74c3c)
    embed.add_field(name="Timer", value=f"{E_TIMER} Ends <t:{end_time}:R>", inline=True)
    embed.add_field(name="Winners", value=f"{E_CROWN} {winners_count}", inline=True)
    if image_url: embed.set_image(url=image_url)
    embed.set_footer(text="React with 🎉 to enter!")
    msg = await ctx.send(embed=embed)
    try: await msg.add_reaction("🎉") 
    except: pass
    view = ParticipantView(msg.id, required_role_ids)
    await msg.edit(view=view)
    await asyncio.sleep(duration_seconds)
    try: msg = await ctx.channel.fetch_message(msg.id)
    except: return
    reaction = None
    target_emoji = discord.PartialEmoji.from_str(E_GIVEAWAY)
    for r in msg.reactions:
         if str(r.emoji) == str(target_emoji) or str(r.emoji) == "🎉":
             reaction = r
             break
    users = []
    if reaction:
        async for user in reaction.users():
            if not user.bot:
                member = ctx.guild.get_member(user.id)
                if member:
                    users.append(member)
    if not users:
        return await msg.reply(embed=create_embed("Ended", "No entrants.", 0x95a5a6))
    
    # Weighted Logic (If weighted=True)
    pool = []
    if weighted:
        for u in users:
            weight = 1
            user_roles = [r.id for r in u.roles]
            for rid, w in DONOR_WEIGHTS.items():
                if rid in user_roles: weight = max(weight, w)
            for _ in range(weight): pool.append(u)
    else:
        pool = users

    final_winners = random.sample(pool, min(len(users), winners_count)) # Sample from pool but unique logic needed if pool has dupes? 
    # Simplify: If weighted, we use random.choices but need unique winners.
    if weighted:
        final_winners = []
        while len(final_winners) < winners_count and len(pool) > 0:
            w = random.choice(pool)
            if w not in final_winners: final_winners.append(w)
            # Remove all instances of w from pool to prevent dupes
            pool = [x for x in pool if x.id != w.id]
    
    winner_mentions = ", ".join([w.mention for w in final_winners])
    tip_amount = 0
    try:
        clean = prize.lower().replace(",", "").replace("$", "")
        if "k" in clean: tip_amount = int(float(clean.replace("k", "")) * 1000)
    except: pass
    tip_msg = ""
    if tip_amount > 0:
        for w in final_winners:
             wallets_col.update_one({"user_id": str(w.id)}, {"$inc": {"balance": tip_amount}}, upsert=True)
        tip_msg = f"\n{E_MONEY} **Auto-Tip:** ${tip_amount:,} sent!"
    await msg.reply(f"Congratulations {winner_mentions}! {tip_msg}")

@bot.hybrid_command(name="giveaway_daily", description="Start Daily giveaway.")
@commands.has_permissions(administrator=True)
async def giveaway_daily(ctx, prize: str, winners: int, duration: str, image: discord.Attachment = None):
    image_url = image.url if image else None
    seconds = parse_duration(duration)
    await run_giveaway(ctx, prize, winners, seconds, "Daily Luck Test!", image_url=image_url)

@bot.hybrid_command(name="giveaway_shiny", description="Start Requirement giveaway.")
@commands.has_permissions(administrator=True)
async def giveaway_shiny(ctx, prize: str, winners: int, duration: str, required_role: discord.Role = None, image: discord.Attachment = None, *, description: str):
    image_url = image.url if image else None
    seconds = parse_duration(duration)
    role_id = required_role.id if required_role else None
    await run_giveaway(ctx, prize, winners, seconds, description, required_role_ids=role_id, weighted=False, image_url=image_url)

@bot.hybrid_command(name="giveaway_donor", description="Start Donor giveaway.")
@commands.has_permissions(administrator=True)
async def giveaway_donor(ctx, prize: str, winners: int, duration: str, image: discord.Attachment = None):
    image_url = image.url if image else None
    seconds = parse_duration(duration)
    await run_giveaway(ctx, prize, winners, seconds, "Donor Weighted!", required_role_ids=list(DONOR_WEIGHTS.keys()), weighted=True, image_url=image_url)

@bot.hybrid_command(name="reroll", aliases=["gr", "giveawayreroll"], description="Admin: Reroll a giveaway winner.")
@commands.has_permissions(administrator=True)
async def reroll(ctx, message_id: str):
    """Rerolls a giveaway in the current channel using the message ID."""
    try:
        msg = await ctx.channel.fetch_message(int(message_id))
    except (discord.NotFound, ValueError):
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Message ID `{message_id}` not found in this channel.", 0xff0000), ephemeral=True)
    
    reaction = None
    target_emoji = discord.PartialEmoji.from_str(E_GIVEAWAY) if E_GIVEAWAY.startswith("<") else "🎉"
    
    # Find the giveaway reaction
    for r in msg.reactions:
        if str(r.emoji) == str(target_emoji) or str(r.emoji) == "🎉":
            reaction = r
            break
            
    if not reaction:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} No valid giveaway reaction found on that message.", 0xff0000), ephemeral=True)
        
    # Fetch users who reacted, filtering out bots
    users = [user async for user in reaction.users() if not user.bot]
    
    if not users:
        return await ctx.send(embed=create_embed(f"{E_ALERT} Reroll Failed", "No valid entrants available to reroll.", 0x95a5a6))
        
    # Pick a new winner
    new_winner = random.choice(users)
    
    # Build the premium announcement embed
    desc = (
        f"{E_CROWN} **New Winner:** {new_winner.mention}\n"
        f"{E_ARROW} **Original Giveaway:** [Jump to Message]({msg.jump_url})"
    )
    
    embed = create_embed(f"{E_GIVEAWAY} Giveaway Rerolled!", desc, 0x2ecc71)
    if new_winner.avatar:
        embed.set_thumbnail(url=new_winner.avatar.url)
        
    await ctx.send(content=f"Congratulations {new_winner.mention}!", embed=embed)

# ===========================
#   GROUP 6: NEW SHOP & INVENTORY
# ===========================

@bot.hybrid_command(name="addshopitem", description="Admin: Add Item to Shop.")
@commands.has_permissions(administrator=True)
async def addshopitem(ctx, name: str, price: int, image: discord.Attachment = None):
    item_id = f"A{get_next_id("shop_item_id")}"
    img_url = image.url if image else None
    shop_items_col.insert_one({
        "id": item_id, "type": "item", "name": name, "price": price, 
        "currency": "shiny", "seller_id": "ADMIN", "image_url": img_url, 
        "sold": False, "tax_exempt": True, "category": "item"
    })
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Added", f"**{name}** added to Admin Shop.\nPrice: {price:,} {E_SHINY}", 0x2ecc71))

@bot.hybrid_command(name="addpokemon", description="Admin: Add Pokemon (Select Category).")
@discord.app_commands.choices(category=[
    discord.app_commands.Choice(name="Common 🍀", value="common"),
    discord.app_commands.Choice(name="Rare 🌌", value="rare"),
    discord.app_commands.Choice(name="Shiny ✨", value="shiny"),
    discord.app_commands.Choice(name="Regional ⛩️", value="regional")
])
@commands.has_permissions(administrator=True)
async def addpokemon(ctx, name: str, level: int, iv: float, price: int, category: discord.app_commands.Choice[str], image: discord.Attachment = None):
    item_id = f"A{get_next_id('shop_item_id')}"
    img_url = image.url if image else None
    
    # Map values to Emojis
    emoji_map = {"common": "🍀", "rare": "🌌", "shiny": "✨", "regional": "⛩️"}
    cat_emoji = emoji_map[category.value]
    
    shop_items_col.insert_one({
        "id": item_id, "type": "pokemon", "name": f"{name} {cat_emoji}", "price": price, 
        "currency": "shiny", "seller_id": "ADMIN", "image_url": img_url, 
        "stats": {"level": level, "iv": iv}, "sold": False, "tax_exempt": True, 
        "category": "pokemon", "sub_category": category.value  # Vital for Mystery Box logic
    })
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Added", f"**{name}** {cat_emoji} (Lvl {level}, {iv}%) added.\nCategory: {category.name}\nID: {item_id}", 0x2ecc71))

@bot.hybrid_command(name="addmysterybox", description="Admin: Add Box (Select Pool).")
@discord.app_commands.choices(pool=[
    discord.app_commands.Choice(name="Common Pool 🍀", value="common"),
    discord.app_commands.Choice(name="Rare Pool 🌌", value="rare"),
    discord.app_commands.Choice(name="Shiny Pool ✨", value="shiny"),
    discord.app_commands.Choice(name="Regional Pool ⛩️", value="regional")
])
@commands.has_permissions(administrator=True)
async def addmysterybox(ctx, name: str, price: int, pool: discord.app_commands.Choice[str]):
    item_id = f"A{get_next_id('shop_item_id')}"
    
    shop_items_col.insert_one({
        "id": item_id, "type": "box", "name": name, "price": price, 
        "currency": "shiny", "seller_id": "ADMIN", "reward_type": pool.value, 
        "sold": False, "tax_exempt": True, "category": "mystery"
    })
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Box Added", f"**{name}** added.\nPool: **{pool.name}**\nPrice: {price:,} {E_SHINY}", 0x2ecc71))

# ==============================================================================
#  MYSTERY BOX LIMIT SYSTEM
# ==============================================================================

@bot.hybrid_command(name="setboxlimit", description="Admin: Limit Mystery Box purchases for a user.")
@commands.has_permissions(administrator=True)
async def setboxlimit(ctx, member: discord.Member, category: str, limit: int, duration: str):
    # 1. Parse Duration (e.g., "24h", "7d")
    seconds = 0
    duration = duration.lower()
    if duration.endswith("d"): seconds = int(duration[:-1]) * 86400
    elif duration.endswith("h"): seconds = int(duration[:-1]) * 3600
    elif duration.endswith("m"): seconds = int(duration[:-1]) * 60
    else: return await ctx.send(embed=create_embed("Error", "Invalid duration. Use `24h`, `7d`, etc.", 0xff0000))
    
    expires_at = datetime.now().timestamp() + seconds
    
    # 2. Normalize Category Name for matching
    valid_cats = ["Shiny", "Rare", "Regional", "Common"]
    target_cat = next((c for c in valid_cats if c.lower() in category.lower()), None)
    
    if not target_cat:
        return await ctx.send(embed=create_embed("Error", f"Invalid category. Choose: {', '.join(valid_cats)}", 0xff0000))
    
    box_name_match = f"{target_cat} Mystery Box"

    # 3. Save to DB
    box_limits_col.update_one(
        {"user_id": str(member.id), "box_name": box_name_match},
        {
            "$set": {
                "limit": limit,
                "bought": 0, # Reset bought count
                "expires_at": expires_at
            }
        },
        upsert=True
    )
    
    desc = (
        f"**User:** {member.mention}\n"
        f"**Box:** {box_name_match}\n"
        f"**Limit:** {limit} purchases\n"
        f"**Expires:** <t:{int(expires_at)}:R>"
    )
    
    await ctx.send(embed=create_embed(f"{E_ADMIN} Limit Set", desc, 0x2ecc71))

@bot.hybrid_command(name="limitinfo", aliases=["limits"], description="Check your Mystery Box buying limits.")
async def limitinfo(ctx):
    # 1. Fetch Limits
    limits = list(box_limits_col.find({"user_id": str(ctx.author.id)}))
    
    active_limits = []
    now = datetime.now().timestamp()
    
    for l in limits:
        # Check if expired
        if now > l['expires_at']:
            box_limits_col.delete_one({"_id": l['_id']}) # Clean up DB
            continue
            
        remaining = l['limit'] - l.get('bought', 0)
        active_limits.append(
            f"**{l['box_name']}**\n"
            f"{E_ITEMBOX} Remaining: **{remaining}/{l['limit']}**\n"
            f"{E_TIMER} Resets: <t:{int(l['expires_at'])}:R>"
        )
    
    if not active_limits:
        return await ctx.send(embed=create_embed(f"{E_GIVEAWAY} Limits", "You have no active purchase limits.", 0x3498db))
    
    await ctx.send(embed=create_embed(f"{E_GIVEAWAY} Your Purchase Limits", "\n\n".join(active_limits), 0xFF69B4))

@bot.hybrid_command(name="addshinycoins", description="Admin: Grant Shiny Coins.")
@commands.has_permissions(administrator=True)
async def addshinycoins(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"shiny_coins": amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Grant", f"Added **{amount:,}** {E_SHINY} to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="removeshopitem", aliases=["rsi", "delitem"], description="Admin: Remove an item from the shop by ID.")
@commands.has_permissions(administrator=True)
async def removeshopitem(ctx, item_id: str):
    # 1. Smart Search to confirm existence
    item = shop_col.find_one({"id": item_id})
    
    # Try legacy integer search if string failed
    if not item and item_id.isdigit():
        item = shop_col.find_one({"id": int(item_id)})
    
    if not item:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Item with ID `{item_id}` not found.", 0xff0000))
    
    # 2. Delete using the unique _id found
    shop_col.delete_one({"_id": item["_id"]})
    
    # 3. Log
    embed = create_embed(f"{E_DANGER} Item Deleted", f"**{item['name']}** (ID: `{item['id']}`) has been removed.", 0xff0000)
    if item.get("image_url"):
        embed.set_thumbnail(url=item["image_url"])
        
    await ctx.send(embed=embed)

@bot.hybrid_command(name="addpc", description="Admin: Grant PC.")
@commands.has_permissions(administrator=True)
async def addpc(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"pc": amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Grant", f"Added **{amount:,}** {E_PC} to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="sellpokemon", aliases=["sp", "listitem"], description="List a Pokemon on the User Market.")
@discord.app_commands.choices(category=[
    discord.app_commands.Choice(name="Common", value="common"),
    discord.app_commands.Choice(name="Rare", value="rare"),
    discord.app_commands.Choice(name="Shiny", value="shiny"),
    discord.app_commands.Choice(name="Regional", value="regional")
])
async def sellpokemon(ctx, name: str, level: int, iv: float, price: int, category: discord.app_commands.Choice[str], image: discord.Attachment = None):
    # Security check for negative or zero prices
    if price <= 0:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Price must be greater than 0.", 0xff0000), ephemeral=True)

    # Generate the unique U-ID sequence for user market items
    item_id = f"U{get_next_id('user_shop_item_id')}"
    img_url = image.url if image else None
    
    # Insert directly into the database
    shop_items_col.insert_one({
        "id": item_id, "type": "pokemon", "name": name, "price": price, 
        "currency": "pc", "seller_id": str(ctx.author.id), "image_url": img_url,
        "stats": {"level": level, "iv": iv}, "sold": False, "tax_exempt": False, 
        "category": "user_market", "sub_category": category.value, "timestamp": datetime.now()
    })
    
    # Premium Embed Output
    desc = (
        f"{E_PC} **Listed For:** {price:,} PC (5% Tax Applies)\n"
        f"{E_ITEMBOX} **ID:** `{item_id}`\n"
        f"{E_STAR} **Category:** {category.name}\n"
        f"{E_BOOST} **Level:** {level} | **IV:** {iv}%"
    )
    
    embed = create_embed(f"{E_SUCCESS} Pokémon Listed Successfully", desc, 0x2ecc71, thumbnail=img_url)
    embed.set_footer(text="Your listing is now live on the User Market!")
    await ctx.send(embed=embed)
# bot.py Part 4 of 4 - Shop UI, Logic, Inventory & Startup
# ... (Continued from Part 3)

# ===========================
#   GROUP 7: SHOP LOGIC & UI
# ===========================

class ShopApprovalView(discord.ui.View):
    def __init__(self, deal_id):
        super().__init__(timeout=None)
        self.deal_id = deal_id

    @discord.ui.button(label="Approve Deal", style=discord.ButtonStyle.success, emoji=discord.PartialEmoji.from_str(E_GOLD_TICK))
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator: return await interaction.response.send_message("Admins only.", ephemeral=True)
        
        deal = pending_shop_approvals.find_one({"id": self.deal_id})
        if not deal: return await interaction.response.send_message("Deal not found/already processed.", ephemeral=True)
        
        # FINAL FUNDS CHECK
        buyer_w = wallets_col.find_one({"user_id": deal['buyer_id']})
        currency_key = "shiny_coins" if deal['currency'] == "shiny" else "pc"
        cost = deal['buyer_pays']
        
        if not buyer_w or buyer_w.get(currency_key, 0) < cost:
            return await interaction.response.send_message(f"{E_ERROR} Buyer no longer has funds!", ephemeral=True)

        # EXECUTE TRANSACTION
        # 1. Deduct from Buyer
        wallets_col.update_one({"user_id": deal['buyer_id']}, {"$inc": {currency_key: -cost}})
        
        # 2. Add to Seller (if not Admin)
        if deal['seller_id'] != "ADMIN":
            wallets_col.update_one({"user_id": deal['seller_id']}, {"$inc": {currency_key: deal['seller_gets']}})
        
        # 3. Transfer Item
        shop_items_col.update_one({"id": deal['item_id']}, {"$set": {"sold": True, "buyer_id": deal['buyer_id']}})
        inventory_col.insert_one({
            "user_id": deal['buyer_id'], "item_id": deal['item_id'], "item_name": deal['item_name'], 
            "price": deal['base_price'], "currency": deal['currency'], "timestamp": datetime.now()
        })
        
        # 4. Notifications
        try:
            buyer = await bot.fetch_user(int(deal['buyer_id']))
            await buyer.send(embed=create_embed(f"{E_SUCCESS} Purchase Approved", f"You bought **{deal['item_name']}** for **{cost:,}** {E_PC if currency_key=='pc' else E_SHINY}!", 0x2ecc71))
        except: pass
        
        if deal['seller_id'] != "ADMIN":
            try:
                seller = await bot.fetch_user(int(deal['seller_id']))
                await seller.send(embed=create_embed(f"{E_SUCCESS} Item Sold", f"Your **{deal['item_name']}** sold! You received **{deal['seller_gets']:,}** {E_PC}.", 0x2ecc71))
            except: pass

        pending_shop_approvals.delete_one({"id": self.deal_id})
        self.stop()
        await interaction.response.edit_message(embed=create_embed(f"{E_SUCCESS} Deal Approved", f"Processed by {interaction.user.mention}.", 0x2ecc71), view=None)

    @discord.ui.button(label="Deny Deal", style=discord.ButtonStyle.danger, emoji=discord.PartialEmoji.from_str(E_ERROR))
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator: return await interaction.response.send_message("Admins only.", ephemeral=True)
        
        deal = pending_shop_approvals.find_one({"id": self.deal_id})
        if deal:
            try:
                buyer = await bot.fetch_user(int(deal['buyer_id']))
                await buyer.send(embed=create_embed(f"{E_DANGER} Purchase Denied", f"Your request for **{deal['item_name']}** was denied.", 0xff0000))
            except: pass
            if deal['seller_id'] != "ADMIN":
                try:
                    seller = await bot.fetch_user(int(deal['seller_id']))
                    await seller.send(embed=create_embed(f"{E_DANGER} Sale Failed", f"The deal for **{deal['item_name']}** was denied by Admin.", 0xff0000))
                except: pass
            pending_shop_approvals.delete_one({"id": self.deal_id})
            
        self.stop()
        await interaction.response.edit_message(embed=create_embed(f"{E_DANGER} Deal Denied", f"Denied by {interaction.user.mention}.", 0xff0000), view=None)

@bot.hybrid_command(name="buy", description="Buy Item (Opt: Use Coupon).")
async def buy(ctx, item_id: str, coupon_code: str = None):
    item = shop_items_col.find_one({"id": item_id, "sold": False})
    if not item: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Item not found or already sold.", 0xff0000))

# ====================================================
    # 👇 MYSTERY BOX LIMIT ENFORCER (Added Here) 👇
    # ====================================================
    # This checks if the item is a Mystery Box and if the user has a limit set
    if item.get("category") == "Mystery Boxes" or item.get("category") == "mystery":
        limit_data = box_limits_col.find_one({
            "user_id": str(ctx.author.id), 
            "box_name": item['name']
        })
        
        if limit_data:
            # Check if limit time has expired
            if datetime.now().timestamp() > limit_data['expires_at']:
                box_limits_col.delete_one({"_id": limit_data["_id"]}) # Remove expired limit
            else:
                # Check if user reached their cap
                current_bought = limit_data.get('bought', 0)
                if (current_bought + 1) > limit_data['limit']:
                    remaining = max(0, limit_data['limit'] - current_bought)
                    return await ctx.send(embed=create_embed(f"{E_DANGER} Limit Reached", f"You have reached your purchase limit for **{item['name']}**.\nYou can buy **{remaining}** more.\nLimit resets <t:{int(limit_data['expires_at'])}:R>.", 0xff0000))
                
                # Increment Count (We update the DB now to reserve the spot)
                box_limits_col.update_one({"_id": limit_data["_id"]}, {"$inc": {"bought": 1}})
    # ====================================================
    # 👆 END OF LIMIT CHECK 👆
    # ====================================================
    
    # Base Costs
    price = item['price']
    currency = item['currency'] # 'pc' or 'shiny'
    is_admin_shop = item['seller_id'] == "ADMIN"
    
    # 1. Calculate Base Tax/Payout
    buyer_pays = price
    seller_gets = price
    tax_info = "None"
    
    if not is_admin_shop and currency == "pc" and not item.get("tax_exempt", False):
        buyer_pays = int(price * (1 + TAX_BUYER_ADD))
        seller_gets = int(price * (1 - TAX_SELLER_SUB))
        tax_info = f"Buyer +2.5% | Seller -2.5%"

    # 2. Apply Coupon Logic (If provided)
    discount_applied = 0
    coupon_msg = ""
    
    if coupon_code:
        cpn = coupons_col.find_one({"code": coupon_code})
        if cpn and cpn['uses'] > 0:
            discount_percent = cpn['discount']
            # Calculate discount on the BUYER's final price
            discount_amount = int(buyer_pays * (discount_percent / 100))
            buyer_pays -= discount_amount
            
            # Decrement Coupon Use
            coupons_col.update_one({"_id": cpn['_id']}, {"$inc": {"uses": -1}})
            
            coupon_msg = f"\n{E_GIVEAWAY} **Coupon:** {coupon_code} (-{discount_percent}%) applied! Saved {discount_amount:,}"
        else:
            return await ctx.send(embed=create_embed("Invalid Coupon", "Code invalid or expired.", 0xff0000))

    # 3. Check Wallet
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    bal = w.get("shiny_coins" if currency == "shiny" else "pc", 0) if w else 0
    
    emoji_curr = E_SHINY if currency == "shiny" else E_PC
    
    if bal < buyer_pays:
        return await ctx.send(embed=create_embed("Insufficient Funds", f"Final Cost: **{buyer_pays:,}** {emoji_curr}\nYou have: **{bal:,}** {emoji_curr}", 0xff0000))
    
    # 4. Create Approval Request
    deal_id = get_next_id("shop_deal_id")
    approval_data = {
        "id": deal_id, "item_id": item['id'], "item_name": item['name'],
        "buyer_id": str(ctx.author.id), "seller_id": item['seller_id'],
        "base_price": price, "buyer_pays": buyer_pays, "seller_gets": seller_gets,
        "currency": currency, "timestamp": datetime.now(), "coupon_used": coupon_code
    }
    pending_shop_approvals.insert_one(approval_data)
    
    # LOG / ADMIN CHANNEL MSG
    log_ch = bot.get_channel(LOG_CHANNELS['shop_log'])
    if log_ch:
        embed_log = create_embed(f"{E_ALERT} Deal Approval Req #{deal_id}", 
                                 f"**Item:** {item['name']} (ID: {item['id']})\n"
                                 f"**Buyer:** {ctx.author.mention}\n"
                                 f"**Seller:** {'ADMIN' if is_admin_shop else f'<@{item['seller_id']}>'}\n"
                                 f"**Base Price:** {price:,} {emoji_curr}\n"
                                 f"**Final Cost:** {buyer_pays:,} {emoji_curr} (Tax: {tax_info}){coupon_msg}", 
                                 0xe67e22, thumbnail=item.get("image_url"))
        await log_ch.send(embed=embed_log, view=ShopApprovalView(deal_id))
    
    # DM Seller
    if not is_admin_shop:
        try:
            seller_user = await bot.fetch_user(int(item['seller_id']))
            await seller_user.send(embed=create_embed(f"{E_PC} Purchase Request", f"User {ctx.author.name} wants to buy **{item['name']}**.\nWaiting for Admin Approval.", 0x3498db))
        except: pass

    try: await update_quest(ctx.author.id, "shop", 1)
    except: pass
    
    await ctx.send(embed=create_embed(f"{E_TIMER} Request Sent", f"Purchase request submitted for **{item['name']}**.\n**Cost:** {buyer_pays:,} {emoji_curr}\n{coupon_msg}\n{E_ADMIN} Waiting for Admin Approval.", 0xf1c40f))

class ShopSelect(Select):
    def __init__(self, ctx):
        options = [
            discord.SelectOption(label="User Market", description="Player listings (PC)", emoji=discord.PartialEmoji.from_str(E_PC), value="user_market"),
            discord.SelectOption(label="Admin: Pokemon", description="Official Pokemon (Shiny Coins)", emoji=discord.PartialEmoji.from_str(E_PIKACHU), value="pokemon"),
            discord.SelectOption(label="Admin: Items", description="Items & Tools (Shiny Coins)", emoji=discord.PartialEmoji.from_str(E_ITEMBOX), value="item"),
            discord.SelectOption(label="Admin: Mystery Boxes", description="Try your luck (Shiny Coins)", emoji=discord.PartialEmoji.from_str(E_GIVEAWAY), value="mystery"),
        ]
        super().__init__(placeholder="Select Shop Category...", min_values=1, max_values=1, options=options)
        self.ctx = ctx

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.ctx.author.id: return
        cat = self.values[0]
        
        query = {"sold": False}
        if cat == "user_market": 
            query["category"] = "user_market"
            emoji_curr = E_PC
            color = 0x3498db
        elif cat == "mystery":
            query["category"] = "mystery"
            emoji_curr = E_SHINY
            color = 0x9b59b6
        else:
            query["category"] = cat
            query["seller_id"] = "ADMIN"
            emoji_curr = E_SHINY
            color = 0xe74c3c
            
        items = list(shop_items_col.find(query))
        if not items: return await interaction.response.send_message(f"No items in **{cat.replace('_', ' ').title()}**.", ephemeral=True)
        
        data = []
        for i in items:
            stats = ""
            if i.get("stats"): stats = f" | Lvl {i['stats']['level']} - {i['stats']['iv']}%"
            data.append((f"{i['name']}{stats}", f"{E_ITEMBOX} ID: **{i['id']}**\n{E_MONEY} Price: **{i['price']:,}** {emoji_curr}"))
            
        view = Paginator(self.ctx, data, f"Shop: {cat.replace('_', ' ').title()}", color)
        await interaction.response.send_message(embed=view.get_embed(), view=view, ephemeral=True)

class ShopView(View):
    def __init__(self, ctx):
        super().__init__(timeout=60)
        self.add_item(ShopSelect(ctx))

@bot.command(name="shop", description="Open Shop Menu.")
async def shop(ctx):
    await ctx.send(embed=create_embed(f"{E_ITEMBOX} Global Market", "Browse the Admin Shop or User Market below.", 0x2ecc71), view=ShopView(ctx))

@bot.hybrid_command(name="marketsearch", description="Filter User Market.")
async def marketsearch(ctx, search: str):
    items = list(shop_items_col.find({"category": "user_market", "sold": False, "name": {"$regex": search, "$options": "i"}}))
    if not items: return await ctx.send(embed=create_embed("Empty", "No items found.", 0x95a5a6))
    data = []
    for i in items:
        stats = f" | Lvl {i['stats']['level']} - {i['stats']['iv']}%" if i.get("stats") else ""
        data.append((f"{i['name']}{stats}", f"ID: **{i['id']}** | Price: **{i['price']:,}** {E_PC}"))
    view = Paginator(ctx, data, f"Search: {search}", 0x3498db)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="itemsearch", aliases=["is"], description="Search Admin Shop items by name.")
async def itemsearch(ctx, search: str):
    items = list(shop_items_col.find({
        "seller_id": "ADMIN", 
        "sold": False, 
        "name": {"$regex": search, "$options": "i"}
    }))
    
    if not items: 
        return await ctx.send(embed=create_embed("Empty", f"No items found matching '**{search}**'.", 0x95a5a6))
        
    data = []
    for i in items:
        stats = f" | Lvl {i['stats']['level']} - {i['stats']['iv']}%" if i.get("stats") else ""
        cat = i.get("category", "Item").title()
        emoji_curr = E_SHINY if i.get("currency") == "shiny" else E_PC
        
        data.append((f"{i['name']}{stats}", f"ID: **{i['id']}** | Type: **{cat}** | Price: **{i['price']:,}** {emoji_curr}"))
        
    view = Paginator(ctx, data, f"Admin Shop Search: {search}", 0xe74c3c)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="pinfo", aliases=["pi"], description="Inspect any Pokemon in the Shop or User Market.")
async def pinfo(ctx, item_id: str):
    # Removed the "seller_id": "ADMIN" filter so it finds ALL Pokemon by ID
    item = shop_items_col.find_one({"id": item_id, "type": "pokemon"})
    
    if not item: 
        return await ctx.send(embed=create_embed("Not Found", f"{E_ERROR} Invalid ID or not a Pokémon.", 0xff0000))
        
    stats = item.get("stats", {"level": 0, "iv": 0})
    cat_raw = item.get("sub_category", "Unknown").title()
    
    # Dynamically determine the seller name and currency emoji
    is_admin = item.get("seller_id") == "ADMIN"
    seller_display = "Admin Shop" if is_admin else f"<@{item['seller_id']}>"
    currency_emoji = E_SHINY if item.get("currency") == "shiny" else E_PC
    status = f"Sold {E_ERROR}" if item.get("sold") else f"Available {E_ACTIVE}"
    
    desc = (
        f"**Name:** {item['name']}\n"
        f"**Category:** {cat_raw}\n"
        f"**Level:** {stats['level']}\n"
        f"**IV:** {stats['iv']}%\n"
        f"**Price:** {item['price']:,} {currency_emoji}\n"
        f"**Seller:** {seller_display}\n"
        f"**Status:** {status}"
    )
    
    embed = create_embed(f"{E_ITEMBOX} Pokémon Inspection", desc, 0x3498db, thumbnail=item.get("image_url"))
    await ctx.send(embed=embed)
    
@bot.hybrid_command(name="iteminfo", aliases=["ii", "pitem"], description="View details of a shop item.")
async def iteminfo(ctx, *, query: str):
    # 1. Search Logic
    item = None

    # Priority 1: Exact String ID match (e.g. "A157")
    item = shop_col.find_one({"id": query})

    # Priority 2: Integer ID match (Legacy IDs like 157)
    if not item and query.isdigit():
        item = shop_col.find_one({"id": int(query)})

    # Priority 3: Name search (Partial match)
    if not item:
        # Escape special regex chars to prevent errors
        safe_query = re.escape(query)
        item = shop_col.find_one({"name": {"$regex": f"^{safe_query}", "$options": "i"}})
        
    if not item:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Item or Box not found.", 0xff0000))

    # 2. Extract Data
    name = item.get('name')
    price = item.get('price')
    image_url = item.get('image_url')
    # Default to 'Item' if category missing, capitalize first letter
    cat = item.get('category', 'item').title()
    
    # Determine Visuals
    currency = E_SHINY if item.get('currency') == 'shiny' else E_PC
    color = 0x3498db # Blue
    
    # Custom Icons based on Category
    icon = E_ITEMBOX
    if "Mystery" in cat: 
        icon = E_GIVEAWAY
        color = 0xFF69B4 # Pink
    elif "Pokemon" in cat: 
        icon = E_PIKACHU
        color = 0xe74c3c # Red

    # 3. Build Premium Embed
    embed = discord.Embed(title=f"{icon} {name}", description=f"**Category:** {cat}", color=color)
    
    embed.add_field(name=f"{currency} Price", value=f"{price:,}", inline=True)
    embed.add_field(name=f"{E_ADMIN} Item ID", value=f"`{item['id']}`", inline=True)
    
    # Stock Display
    stock = item.get('stock', -1)
    stock_str = "∞ (Unlimited)" if stock == -1 else f"{stock:,}"
    embed.add_field(name=f"{E_ITEMBOX} Stock", value=stock_str, inline=True)

    # 4. Load Image (Crucial)
    if image_url:
        embed.set_thumbnail(url=image_url)
    
    await ctx.send(embed=embed)

@bot.command(name="inventory", aliases=["inv"], description="View Items & Balance.")
async def inventory(ctx):
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    shiny = w.get("shiny_coins", 0) if w else 0
    pc = w.get("pc", 0) if w else 0
    items = list(inventory_col.find({"user_id": str(ctx.author.id)}))
    
    desc = f"{E_SHINY} **Shiny Coins:** {shiny:,}\n{E_PC} **Pokécoins:** {pc:,}\n\n**Your Items:**"
    data = []
    if not items: data.append(("Empty", "No items owned."))
    else:
        for i in items: data.append((f"{i['item_name']}", f"Bought for: {i['price']:,} {i.get('currency', 'pc')}"))
        
    view = Paginator(ctx, data, f"{E_ITEMBOX} Inventory: {ctx.author.name}", 0x9b59b6)
    # Inject header into first page
    first_embed = view.get_embed()
    first_embed.description = desc
    await ctx.send(embed=first_embed, view=view)

@bot.command(name="boxes", description="Check your PC Box inventory.")
async def boxes(ctx):
    w = get_wallet(ctx.author.id)
    box_count = w.get("pc_boxes", 0)
    
    desc = (
        f"You currently have **{box_count:,}** {E_ITEMBOX} **PC Boxes**.\n\n"
        f"Use `.openbox <amount>` to open them and earn {E_PC} Pokécoins!"
    )
    
    embed = create_embed(f"{E_ITEMBOX} Your PC Boxes", desc, 0x3498db)
    if ctx.author.avatar:
        embed.set_thumbnail(url=ctx.author.avatar.url)
        
    await ctx.send(embed=embed)

@bot.command(name="openbox", aliases=["ob"], description="Open your PC Boxes.")
async def openbox(ctx, amount: int = 1):
    if amount <= 0:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You must open at least 1 box.", 0xff0000), ephemeral=True)
        
    w = get_wallet(ctx.author.id)
    box_count = w.get("pc_boxes", 0)
    
    if box_count < amount:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You only have **{box_count:,}** PC Boxes.", 0xff0000), ephemeral=True)
        
    # Calculate random rewards (30k to 150k per box)
    total_pc = sum(random.randint(30000, 150000) for _ in range(amount))
    
    # Update Database (Deduct boxes, Add PC)
    wallets_col.update_one(
        {"user_id": str(ctx.author.id)},
        {"$inc": {"pc_boxes": -amount, "pc": total_pc}}
    )
    
    desc = (
        f"{E_SUCCESS} Successfully opened **{amount:,}** PC Box(es)!\n\n"
        f"{E_PC} **Reward:** {total_pc:,} Pokécoins\n"
        f"{E_ITEMBOX} **Remaining Boxes:** {box_count - amount:,}"
    )
    
    embed = create_embed(f"{E_GIVEAWAY} Boxes Opened!", desc, 0x2ecc71)
    if ctx.author.avatar:
        embed.set_thumbnail(url=ctx.author.avatar.url)
        
    await ctx.send(embed=embed)

# ===========================
#   PC DEPOSIT    SYSTEM
# ===========================

@bot.command(name="depositpc", aliases=["dpc"])
async def depositpc(ctx):
    # 1. The Lock Check (Defaults to True/Open if not touched)
    if getattr(bot, 'deposits_open', True) is False:
        desc = f"{E_ERROR} The PC deposit system is currently **closed**.\n\nPlease request a server Owner or Staff member to open it."
        return await ctx.send(embed=create_embed("Deposits Offline", desc, 0xff0000))

    # 2. The Normal Command (If Open)
    desc = "Click the button below to fill out the deposit form.\n\n⚠️ Ensure your DMs are open so the bot can send you the Market ID."
    await ctx.send(embed=create_embed("Bank Deposit", desc, 0x3498db), view=DepositView())
    
@bot.command(name="opendeposits")
@commands.has_permissions(administrator=True)
async def opendeposits(ctx):
    bot.deposits_open = True # Flips the switch ON
    desc = f"{E_SUCCESS} The PC deposit system has been successfully **OPENED**.\nUsers can now use `.dpc` again."
    await ctx.send(embed=create_embed("System Opened", desc, 0x2ecc71))

@bot.command(name="closedeposits")
@commands.has_permissions(administrator=True)
async def closedeposits(ctx):
    bot.deposits_open = False # Flips the switch OFF
    desc = f"{E_ALERT} The PC deposit system is now **CLOSED**.\nUsers will be blocked from using `.dpc` until reopened."
    await ctx.send(embed=create_embed("System Closed", desc, 0xff0000))

@bot.command(name="depositpcstatus", aliases=["dpcs"])
async def depositpcstatus(ctx, member: discord.Member = None):
    target = member or ctx.author
    deps = list(deposits_col.find({"user_id": str(target.id)}).sort("created_at", -1))
    
    if not deps:
        return await ctx.send(embed=create_embed("No Deposits", f"{E_ERROR} No records found for {target.mention}.", 0xff0000))

    total_pc = sum(d["amount"] for d in deps if d["status"] == "Completed")
    desc = f"**Total Deposited:** {E_MONEY} **{total_pc:,} PC**\n\n**Recent Deposits:**\n"
    
    for d in deps[:5]: # Show last 5
        status_emoji = E_SUCCESS if d["status"] == "Completed" else (E_ALERT if d["status"] == "On Hold" else "⏳")
        td = datetime.now(timezone.utc) - d["created_at"].replace(tzinfo=timezone.utc)
        elapsed = f"{int(td.total_seconds() // 60)}m {int(td.total_seconds() % 60)}s"
        
        desc += f"▫️ `{d['deposit_id']}` | **{d['amount']:,} PC** | {status_emoji} {d['status']} | Time: {elapsed}\n"

    await ctx.send(embed=create_embed(f"Status: {target.display_name}", desc, 0x9b59b6))

@bot.command(name="pendingdeposits", aliases=["pdpc"])
@commands.has_permissions(administrator=True)
async def pendingdeposits(ctx):
    deps = list(deposits_col.find({"status": {"$in": ["Queued", "On Hold"]}}).sort("created_at", 1))
    if not deps:
        return await ctx.send(embed=create_embed("Queue Clear", f"{E_SUCCESS} There are no pending deposits.", 0x2ecc71))

    desc = ""
    for d in deps:
        td = datetime.now(timezone.utc) - d["created_at"].replace(tzinfo=timezone.utc)
        elapsed = f"{int(td.total_seconds() // 60)}m"
        desc += f"`{d['deposit_id']}` | <@{d['user_id']}> | **{d['amount']:,} PC** | {d['status']} ({elapsed})\n"

    await ctx.send(embed=create_embed("Pending Deposits", desc, 0xe67e22))

@bot.command(name="logdepositpc")
@commands.has_permissions(administrator=True)
async def logdepositpc(ctx, deposit_id: str, status: str):
    dep_id = deposit_id.lower()
    stat = status.capitalize()
    if stat not in ["Approved", "Rejected"]:
        return await ctx.send(embed=create_embed("Error", "Status must be `approved` or `rejected`.", 0xff0000))
        
    deposit = deposits_col.find_one({"deposit_id": dep_id})
    if not deposit: 
        return await ctx.send(embed=create_embed("Error", "Deposit ID not found.", 0xff0000))

    new_status = "Completed" if stat == "Approved" else "Rejected"
    deposits_col.update_one({"deposit_id": dep_id}, {"$set": {"status": new_status}})

    if stat == "Approved":
        wallets_col.update_one({"user_id": deposit["user_id"]}, {"$inc": {"pc": deposit["amount"]}}, upsert=True)

    # Log it
    log_channel = bot.get_channel(1483526389339521066)
    if log_channel:
        embed = discord.Embed(title=f"Manual Log: {stat}", description=f"**ID:** `{dep_id}`\n**User:** <@{deposit['user_id']}>\n**Amount:** {deposit['amount']:,} PC", color=0x2ecc71 if stat == "Approved" else 0xff0000)
        await log_channel.send(embed=embed)

    await ctx.send(embed=create_embed("Success", f"Deposit `{dep_id}` manually marked as **{stat}**.", 0x2ecc71))

# ===========================
#   REWARDS & CODES SYSTEM
# ===========================

@bot.hybrid_command(name="create_coupon", aliases=["cc"], description="Admin: Create Discount Code.")
@commands.has_permissions(administrator=True)
async def create_coupon(ctx, code: str, discount_percent: int, uses: int):
    coupons_col.insert_one({"code": code, "discount": discount_percent, "uses": uses})
    await ctx.send(embed=create_embed(f"{E_ADMIN} Coupon Created", f"Code: **{code}**\nDiscount: **{discount_percent}%**\nUses: **{uses}**", 0xe67e22))

@bot.hybrid_command(name="coupon", description="Check/Use a Coupon.")
async def coupon(ctx, code: str):
    # Validates and burns a use. Useful for manual deals or giveaways.
    c = coupons_col.find_one({"code": code})
    if not c or c['uses'] <= 0:
        return await ctx.send(embed=create_embed("Invalid", "Coupon invalid or expired.", 0xff0000))

    coupons_col.update_one({"_id": c['_id']}, {"$inc": {"uses": -1}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Coupon Valid", f"Code **{code}** redeemed!\n**{c['discount']}% OFF** applied (Manual Deal).", 0x2ecc71))

@bot.hybrid_command(name="create_redeem", aliases=["crc"], description="Admin: Create Currency Code.")
@commands.has_permissions(administrator=True)
async def create_redeem(ctx, type: str, amount: int, uses: int = 1):
    if type.lower() not in ['pc', 'shiny']: return await ctx.send("Type must be 'pc' or 'shiny'.")
    
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    
    # NEW STRUCTURE: Stores max_uses and a list of users who claimed it
    redeem_codes_col.insert_one({
        "code": code, 
        "type": type.lower(), 
        "amount": amount, 
        "max_uses": uses, 
        "claimed_users": [] 
    })
    
    emoji = E_SHINY if type.lower() == 'shiny' else E_PC
    try:
        await ctx.author.send(embed=create_embed(f"{E_ADMIN} Code Created", f"Code: `{code}`\nValue: **{amount:,}** {emoji}\nUses: **{uses}**", 0xe67e22))
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Created", "Code sent to your DMs.", 0x2ecc71))
    except:
        await ctx.send(embed=create_embed(f"{E_ADMIN} Code Created", f"Code: `{code}`\nValue: **{amount:,}** {emoji}\nUses: **{uses}**", 0xe67e22))

@bot.hybrid_command(name="redeem", aliases=["rcode"], description="Claim a Currency Code.")
async def redeem(ctx, code: str):
    r = redeem_codes_col.find_one({"code": code})
    
    # 1. Validation Checks
    if not r: 
        return await ctx.send(embed=create_embed("Error", "Invalid code.", 0xff0000))
    
    if len(r.get("claimed_users", [])) >= r.get("max_uses", 1):
        return await ctx.send(embed=create_embed("Error", "Code fully claimed.", 0xff0000))
        
    if str(ctx.author.id) in r.get("claimed_users", []):
        return await ctx.send(embed=create_embed("Error", "You already claimed this.", 0xff0000))
    
    # 2. Process Claim
    curr_key = "shiny_coins" if r['type'] == 'shiny' else "pc"
    emoji = E_SHINY if r['type'] == 'shiny' else E_PC
    
    # Add user to claimed list
    redeem_codes_col.update_one({"_id": r['_id']}, {"$push": {"claimed_users": str(ctx.author.id)}})
    
    # Give Money
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {curr_key: r['amount']}}, upsert=True)
    
    # 3. Success Message (Shows remaining uses)
    uses_left = r['max_uses'] - (len(r.get("claimed_users", [])) + 1)
    await ctx.send(embed=create_embed(f"{E_GIVEAWAY} Redeemed!", f"You claimed **{r['amount']:,}** {emoji}!\n({uses_left} claims remaining)", 0x2ecc71))

@bot.hybrid_command(name="use", description="Use an Item/Box.")
async def use(ctx, item_name: str):
    # 1. Find box in inventory
    inv_item = inventory_col.find_one({"user_id": str(ctx.author.id), "item_name": {"$regex": f"^{item_name}$", "$options": "i"}})
    if not inv_item: return await ctx.send(embed=create_embed("Error", "Item not in inventory.", 0xff0000))
    
    shop_ref = shop_items_col.find_one({"id": inv_item.get("item_id")})
    if not shop_ref or shop_ref.get("category") != "mystery":
        return await ctx.send(embed=create_embed("Info", "This item cannot be 'used'.", 0x95a5a6))
    
    pool_type = shop_ref.get("reward_type", "common")
    
    # 2. Find Random Prize
    candidates = list(shop_items_col.find({
        "type": "pokemon", "seller_id": "ADMIN", "sold": False, "sub_category": pool_type
    }))
    
    if not candidates:
        return await ctx.send(embed=create_embed("Stock Empty", f"No {pool_type.title()} Pokemon available in Admin Shop! Ask an Admin to restock.", 0xff0000))
    
    prize = random.choice(candidates)
    
    # 3. Transfer Prize
    shop_items_col.update_one({"_id": prize["_id"]}, {"$set": {"sold": True, "buyer_id": str(ctx.author.id)}})
    
    inventory_col.insert_one({
        "user_id": str(ctx.author.id), "item_id": prize["id"], "item_name": prize["name"], 
        "price": 0, "currency": "reward", "timestamp": datetime.now(), "stats": prize.get("stats")
    })
    
    # 4. Burn Box
    inventory_col.delete_one({"_id": inv_item["_id"]})
    
    embed = create_embed(f"{E_GIVEAWAY} Box Opened!", f"You opened **{inv_item['item_name']}** and found:\n# {prize['name']}", 0x2ecc71)
    if prize.get("image_url"): embed.set_thumbnail(url=prize["image_url"])
    await ctx.send(embed=embed)

# ==============================================================================
#  GROUP 8: DUELISTS SYSTEM COMMANDS
# ==============================================================================

@bot.command(name="duelistinfo", aliases=["di"], description="View a Duelist's profile and stats.")
async def duelistinfo(ctx, identifier: str = None):
    target_user = ctx.author
    
    if identifier:
        duelist = get_duelist(identifier)
        if not duelist:
            return await ctx.send(embed=create_embed("Not Found", f"{E_ERROR} Duelist not found. Ensure they are registered.", 0xff0000))
    else:
        duelist = duelists_col.find_one({"user_id": str(ctx.author.id)})
        if not duelist:
            return await ctx.send(embed=create_embed("Not Registered", f"{E_ERROR} You are not registered as a duelist.", 0xff0000))

    try: target_user = await bot.fetch_user(int(duelist["user_id"]))
    except: pass

    club_name = "Free Agent"
    if duelist.get("club_id"):
        club = clubs_col.find_one({"_id": duelist["club_id"]})
        if club: club_name = club["name"]

   # Fetch wallet for Ballon d'Or stats
    w = get_wallet(target_user.id)
    awards_text = ""
    if w.get("t_ballondor", 0) > 0:
        b = w["t_ballondor"]
        awards_text += f"\n\n**Ballon d'Ors**\n{b}x " + (E_BALLONDOR * b)
    if w.get("t_sballondor", 0) > 0:
        sb = w["t_sballondor"]
        awards_text += f"\n\n**Super Ballon d'Ors**\n{sb}x " + (E_SUPERBALLONDOR * sb)

    desc = (
        f"{E_CROWN} **Duelist ID:** `{duelist.get('duelist_id', 'N/A')}`\n"
        f"{E_ADMIN} **Status:** {duelist.get('status', 'Free Agent')}\n"
        f"{E_BOOST} **Current Club:** {club_name}\n"
        f"{E_MONEY} **Market Value:** ${duelist.get('market_worth', 100000):,}\n\n"
        f"**🏆 Match Statistics**\n"
        f"**Matches:** {duelist.get('matches', 0)} | **MVPs:** {duelist.get('mvps', 0)}\n"
        f"**Wins:** {duelist.get('wins', 0)} | **Losses:** {duelist.get('losses', 0)} | **Draws:** {duelist.get('draws', 0)}"
        f"{awards_text}" # <--- THIS ADDS THE TROPHIES AT THE BOTTOM!
    )
    
    embed = create_embed(f"{E_STARS} Duelist Profile: {target_user.display_name}", desc, 0x3498db)
    if target_user.avatar: embed.set_thumbnail(url=target_user.avatar.url)
    await ctx.send(embed=embed)


@bot.command(name="duelistleaderboard", aliases=["dlb"], description="View top duelists by market worth.")
async def duelistleaderboard(ctx):
    duelists = list(duelists_col.find({"status": {"$ne": "Left the Server"}}).sort("market_worth", -1).limit(50))
    if not duelists: return await ctx.send("No duelists registered.")
        
    data = []
    for i, d in enumerate(duelists):
        title = f"#{i+1} | ID: {d.get('duelist_id')} | <@{d['user_id']}>"
        desc = f"{E_MONEY} **Worth:** ${d.get('market_worth', 0):,} | {E_STARS} **Wins:** {d.get('wins', 0)}"
        data.append((title, desc))
        
    view = Paginator(ctx, data, f"{E_CROWN} Top Duelists", 0xf1c40f)
    await ctx.send(embed=view.get_embed(), view=view)


@bot.command(name="battledrew", aliases=["bdrew"], description="Admin: Mark a battle as a draw.")
@commands.has_permissions(administrator=True)
async def battledrew(ctx, battle_id: str):
    # Fetch battle and clubs involved (Assuming you have a battles collection)
    # Give +1 Draw, +1 Match, +$25k to all duelists in both clubs
    duelists_col.update_many(
        {"club_id": {"$in": ["CLUB_1_ID_HERE", "CLUB_2_ID_HERE"]}}, # Replace with actual logic to fetch clubs from battle
        {"$inc": {"draws": 1, "matches": 1, "market_worth": 25000}}
    )
    await ctx.send(embed=create_embed("Battle Drawn", f"{E_SUCCESS} Battle `{battle_id}` marked as a draw. All duelists updated.", 0x2ecc71))


@bot.command(name="battlemvp", aliases=["bmvp"], description="Admin: Award MVP for a battle.")
@commands.has_permissions(administrator=True)
async def battlemvp(ctx, battle_id: str, duelist_identifier: str):
    duelist = get_duelist(duelist_identifier)
    if not duelist: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Duelist not found.", 0xff0000))
        
    duelists_col.update_one({"_id": duelist["_id"]}, {"$inc": {"mvps": 1, "market_worth": 100000}})
    await ctx.send(embed=create_embed("MVP Awarded", f"{E_STARS} <@{duelist['user_id']}> awarded MVP for Battle `{battle_id}`! Market worth increased by $100k.", 0xf1c40f))


@bot.command(name="removeduelist", aliases=["rdc"], description="Admin: Remove a duelist from their club.")
@commands.has_permissions(administrator=True)
async def removeduelist(ctx, duelist_identifier: str):
    duelist = get_duelist(duelist_identifier)
    if not duelist: return await ctx.send("Duelist not found.")
        
    duelists_col.update_one({"_id": duelist["_id"]}, {"$set": {"club_id": None, "status": "Free Agent"}})
    await ctx.send(embed=create_embed("Duelist Removed", f"{E_SUCCESS} <@{duelist['user_id']}> is now a Free Agent.", 0x2ecc71))


@bot.command(name="deleteduelist", aliases=["ddlist"], description="Admin: Delete a duelist profile permanently.")
@commands.has_permissions(administrator=True)
async def deleteduelist(ctx, duelist_identifier: str):
    duelist = get_duelist(duelist_identifier)
    if not duelist: return await ctx.send("Duelist not found.")
        
    duelists_col.delete_one({"_id": duelist["_id"]})
    await ctx.send(embed=create_embed("Profile Erased", f"{E_SUCCESS} Duelist profile for <@{duelist['user_id']}> has been completely wiped from the database.", 0xff0000))

@bot.hybrid_command(name="setoffline", aliases=["leftserver", "markleft"], description="Admin: Manually mark a duelist as 'Left the Server' and refund their club.")
@commands.has_permissions(administrator=True)
async def setoffline(ctx, duelist_identifier: str):
    duelist = get_duelist(duelist_identifier)
    if not duelist:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Duelist not found.", 0xff0000))
        
    if duelist.get("status") == "Left the Server":
        return await ctx.send(embed=create_embed("Already Offline", f"{E_ERROR} This duelist is already marked as Left the Server.", 0xf1c40f))

    club_id = duelist.get("club_id")
    refund_amount = 0
    club_name = "None"
    
    # Process Club Refund if they were signed
    if club_id:
        club = clubs_col.find_one({"_id": club_id}) or clubs_col.find_one({"id": club_id})
        if club:
            club_name = club.get('name', 'Unknown Club')
            # Refund their live market worth back to the owner
            refund_amount = duelist.get("market_worth", 100000) 
            owner_id = club.get("owner_id")
            
            if owner_id:
                if str(owner_id).startswith("group:"):
                    gname = str(owner_id).replace("group:", "")
                    db.groups.update_one({"name": gname}, {"$inc": {"funds": refund_amount}})
                else:
                    wallets_col.update_one({"user_id": str(owner_id)}, {"$inc": {"balance": refund_amount}})
                    
    # Update duelist status
    duelists_col.update_one(
        {"_id": duelist["_id"]}, 
        {"$set": {"status": "Left the Server", "club_id": None, "transfer_listed": False}}
    )
    
    # Cleanup any pending ghost contracts or transfer offers
    if db is not None:
        db.contracts.delete_many({"duelist_id": duelist["_id"]})
        db.pending_contracts.delete_many({"duelist_id": duelist["_id"]})
        db.pending_transfers.delete_many({"duelist_id": duelist["_id"]})
    
    desc = f"{E_SUCCESS} <@{duelist['user_id']}> has been manually marked as **Left the Server**."
    if refund_amount > 0:
        desc += f"\n\n{E_MONEY} **Refund Issued:** **${refund_amount:,}** was refunded to **{club_name}**'s owner/group funds."
        
    await ctx.send(embed=create_embed(f"{E_ADMIN} Duelist Offline", desc, 0xe74c3c))

# ==============================================================================
#  PHASE 2: TRANSFER MARKET & CONTRACT COMMANDS
# ==============================================================================

@bot.command(name="requesttransfer", aliases=["rtransfer", "rt"], description="Request to be added to the Transfer Market.")
async def requesttransfer(ctx):
    duelist = duelists_col.find_one({"user_id": str(ctx.author.id)})
    if not duelist: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You are not a registered duelist.", 0xff0000))
        
    current_status = duelist.get("transfer_listed", False)
    duelists_col.update_one({"_id": duelist["_id"]}, {"$set": {"transfer_listed": not current_status}})
    
    status_msg = "ADDED TO" if not current_status else "REMOVED FROM"
    await ctx.send(embed=create_embed("Transfer Market", f"{E_SUCCESS} You have been **{status_msg}** the Transfer Market.", 0x2ecc71))

@bot.command(name="transfermarket", aliases=["tm"], description="View duelists seeking a transfer.")
async def transfermarket(ctx):
    duelists = list(duelists_col.find({"transfer_listed": True}))
    if not duelists: return await ctx.send(embed=create_embed("Transfer Market", f"{E_ERROR} The transfer market is currently empty.", 0x95a5a6))
        
    data = []
    for d in duelists:
        title = f"ID: {d.get('duelist_id')} | <@{d['user_id']}>"
        desc = f"{E_MONEY} **Value:** ${d.get('market_worth', 100000):,}\n{E_STARS} **Wins:** {d.get('wins', 0)} | **Matches:** {d.get('matches', 0)}"
        data.append((title, desc))
        
    view = Paginator(ctx, data, f"{E_ADMIN} Live Transfer Market", 0x3498db)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.command(name="transferbuy", aliases=["tb"], description="Club Owner: Buy a duelist from the transfer market.")
async def transferbuy(ctx, duelist_id: str):
    buyer_club = clubs_col.find_one({"owner_id": str(ctx.author.id)})
    if not buyer_club: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You must own a club to buy a duelist.", 0xff0000))
        
    duelist = get_duelist(duelist_id)
    if not duelist or not duelist.get("transfer_listed"): 
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Duelist not found or not on the transfer market.", 0xff0000))
        
    price = duelist.get("market_worth", 100000)
    old_club = clubs_col.find_one({"_id": duelist["club_id"]}) if duelist.get("club_id") else None
    
    # Send UI to Duelist
    try:
        duelist_user = await bot.fetch_user(int(duelist["user_id"]))
        desc = f"**{buyer_club['name']}** wants to buy you from the Transfer Market for **${price:,}**!"
        view = TransferBuyView(buyer_club, old_club, duelist, price)
        await duelist_user.send(embed=create_embed(f"{E_CROWN} Transfer Offer", desc, 0xf1c40f), view=view)
        await ctx.send(embed=create_embed("Offer Sent", f"{E_SUCCESS} Transfer offer sent to <@{duelist['user_id']}>'s DMs!", 0x2ecc71))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"{E_ERROR} Could not DM the duelist. They must open their DMs.", 0xff0000))

@bot.command(name="contract", aliases=["signup"], description="Club Owner: Offer a contract to a duelist.")
async def contract(ctx, duelist_identifier: str, seasons: int, role: str):
    """Usage: .contract @user 3 Crucial"""
    club = clubs_col.find_one({"owner_id": str(ctx.author.id)})
    if not club: return await ctx.send("You don't own a club.")
        
    duelist = get_duelist(duelist_identifier)
    if not duelist: return await ctx.send("Duelist not found.")
        
    if seasons < 1 or seasons > 5: return await ctx.send("Seasons must be between 1 and 5.")
    valid_roles = ["crucial", "rotational", "sub"]
    if role.lower() not in valid_roles: return await ctx.send("Role must be: Crucial, Rotational, or Sub.")

    # Send Modal for Salary
    view = discord.ui.View()
    button = discord.ui.Button(label="Set Salary & Send Offer", style=discord.ButtonStyle.success)
    
    async def button_callback(interaction: discord.Interaction):
        if interaction.user.id != ctx.author.id: return
        await interaction.response.send_modal(ContractSalaryModal(club, duelist, seasons, role.capitalize()))
        
    button.callback = button_callback
    view.add_item(button)
    
    desc = f"**Duelist:** <@{duelist['user_id']}>\n**Seasons:** {seasons}\n**Role:** {role.capitalize()}\n\nClick below to set the salary."
    await ctx.send(embed=create_embed("Contract Wizard", desc, 0x3498db), view=view)

@bot.command(name="contractinfo", aliases=["tci"], description="View details of a duelist's active contract.")
async def contractinfo(ctx, duelist_identifier: str):
    duelist = get_duelist(duelist_identifier)
    if not duelist: return await ctx.send("Duelist not found.")
        
    cnt = db.contracts.find_one({"duelist_id": duelist["_id"]})
    if not cnt: return await ctx.send(embed=create_embed("No Contract", f"<@{duelist['user_id']}> does not have an active contract.", 0xff0000))
        
    club = clubs_col.find_one({"_id": cnt["club_id"]})
    club_name = club["name"] if club else "Unknown Club"
    
    desc = (
        f"{E_CROWN} **Club:** {club_name}\n"
        f"{E_STARS} **Role:** {cnt.get('role', 'N/A')}\n"
        f"{E_TIMER} **Seasons Left:** {cnt.get('seasons', 0)}\n\n"
        f"**Salary per Season:**\n{E_MONEY} **${cnt.get('cash_salary', 0):,}**\n{E_PC} **{cnt.get('pc_salary', 0):,}** PC"
    )
    await ctx.send(embed=create_embed(f"{E_BOOK} Contract Info: <@{duelist['user_id']}>", desc, 0x9b59b6))

@bot.command(name="seasonend", aliases=["sed"], description="Admin: Advance the season by 1. Updates all contracts.")
@commands.has_permissions(administrator=True)
async def seasonend(ctx):
    # 1. Deduct 1 season from all active contracts
    db.contracts.update_many({}, {"$inc": {"seasons": -1}})
    
    # 2. Find expired contracts (Seasons <= 0)
    expired = list(db.contracts.find({"seasons": {"$lte": 0}}))
    expired_ids = [c["duelist_id"] for c in expired]
    
    # 3. Make them Free Agents & Delete contracts
    if expired_ids:
        duelists_col.update_many({"_id": {"$in": expired_ids}}, {"$set": {"status": "Free Agent", "club_id": None}})
        db.contracts.delete_many({"_id": {"$in": [c["_id"] for c in expired]}})
        
    desc = f"{E_SUCCESS} The Season has officially ended!\nAll contracts have been reduced by 1 season.\n**{len(expired)}** duelists have finished their contracts and entered Free Agency."
    await ctx.send(embed=create_embed(f"{E_ADMIN} Season Advancement", desc, 0x2ecc71))

# --- START OF HELP MENU & BOTINFO ---

class HelpSelect(Select):
    def __init__(self):
        # This defines the dropdown options using your CUSTOM EMOJIS
        options = [
            discord.SelectOption(label="Economy & Groups", emoji=discord.PartialEmoji.from_str(E_MONEY), description="Wallet, Groups, Banking", value="economy"),
            discord.SelectOption(label="Football & Roles", emoji=discord.PartialEmoji.from_str(E_FIRE), description="Clubs, Duelists, Salaries", value="football"),
            discord.SelectOption(label="Club Market", emoji=discord.PartialEmoji.from_str(E_AUCTION), description="Auctions, Buying, Selling", value="market"),
            discord.SelectOption(label="Pokémon Shop", emoji=discord.PartialEmoji.from_str(E_PC), description="Shop, Inventory, Rewards", value="shop"),
            discord.SelectOption(label="Admin Tools", emoji=discord.PartialEmoji.from_str(E_ADMIN), description="Staff Commands Only", value="admin"),
            discord.SelectOption(label="Updates (v5.8)", emoji=discord.PartialEmoji.from_str(E_BOOST), description="Patch Notes", value="updates")
        ]
        super().__init__(placeholder="Select a Help Category...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        val = self.values[0]
        embed = discord.Embed(color=0x3498db)
        
        # This defines the CONTENT inside the categories
        if val == "economy":
            embed.title = f"{E_MONEY} Economy Guide"
            embed.description = "Manage finances."
            embed.color = 0x2ecc71
            embed.add_field(name=f"{E_MONEY} **Personal Finance**", value=f"`.wl` **Wallet:** Check balance.\n`.ww <Amt>` **Withdraw Wallet:** Burn/delete money.", inline=False)
            embed.add_field(name=f"{E_PREMIUM} **Groups**", value=f"`.cg <Name> <%>` **Create Group:** Start new group.\n`.jg <Name> <%>` **Join Group:** Join group.\n`.gi <Name>` **Info:** Funds & members.\n`.gl` **List:** All groups.\n`.lg <Name>` **Leave:** Exit group (10% penalty).", inline=False)
            embed.add_field(name=f"{E_BOOST} **Banking**", value=f"`.dep <Grp> <Amt>` **Deposit:** Wallet → Group.\n`.wd <Grp> <Amt>` **Withdraw:** Group → Wallet.", inline=False)

        elif val == "football":
            embed.title = f"{E_FIRE} Football Features"
            embed.description = "Manage clubs & players."
            embed.color = 0x3498db
            embed.add_field(name=f"{E_CROWN} **Clubs**", value=f"`.ci <Club>` **Info:** Owner, Value, Wins.\n`.cl <Club>` **Level:** Division progress.\n`.lc` **List:** All clubs.\n`.lb` **Leaderboard:** Global ranks.", inline=False)
            embed.add_field(name=f"{E_ITEMBOX} **Duelists**", value=f"`.rd <Name> <Price> <Sal>` **Register:** Join as player.\n`.ld` **List:** Available players.\n`.ret` **Retire:** Delete profile.", inline=False)
            embed.add_field(name=f"{E_ADMIN} **Owner Tools**", value=f"`.as <ID> <Amt>` **Salary:** Pay bonus/fine.\n`.ds <ID> yes` **Deduct:** Fine 15% for miss.", inline=False)

        elif val == "market":
            embed.title = f"{E_AUCTION} Club Market"
            embed.description = "Buy, Sell & Trade."
            embed.color = 0xe67e22
            embed.add_field(name=f"{E_AUCTION} **Trading**", value=f"`.ml` **Market List:** Unsold clubs.\n`.bc <Club>` **Buy Club:** Request purchase (User).\n`.gbc <Grp> <Club>` **Group Buy:** Request purchase (Group).\n`.sc <Club>` **Sell:** To Market or User.\n`.ss <Club> <User> <%>` **Shares:** Sell Group %.", inline=False)
            embed.add_field(name=f"{E_TIMER} **Auctions**", value=f"`.pb <Amt> <Type> <ID>` **Bid:** Place bid.\n`.gb <Grp> <Amt> <Type> <ID>` **Group Bid:** Bid with group funds.", inline=False)
            embed.add_field(name=f"{E_STARS} **Analysis**", value=f"`.mp <Club>` **Panel:** Financial stats.", inline=False)

        elif val == "shop":
            embed.title = f"{E_PC} Pokémon Market"
            embed.description = "Buy & Sell Items."
            embed.color = 0x9b59b6
            embed.add_field(name=f"{E_SHINY} **Admin Shop**", value=f"`.shop` **Menu:** Open Shop UI.\n`.buy <ID>` **Buy:** Purchase item (Req Approval).", inline=False)
            embed.add_field(name=f"{E_PC} **User Shop**", value=f"`.sellpokemon <Price>` **List:** Sell Pokétwo Pokémon.\n`.marketsearch <Query>` **Search:** Find user items.", inline=False)
            embed.add_field(name=f"{E_ITEMBOX} **Inventory**", value=f"`.inv` **Inventory:** View items & coins.\n`.use <Item>` **Use:** Open Mystery Boxes.\n`.buycoins <Amt>` **Exchange:** Cash → Shiny Coins.", inline=False)

        elif val == "admin":
            if not interaction.user.guild_permissions.administrator: return await interaction.response.send_message(f"{E_ERROR} Staff Only.", ephemeral=True)
            embed.title = f"{E_ADMIN} Staff Commands"
            embed.description = "Admin Control Panel."
            embed.color = 0xff0000
            embed.add_field(name=f"{E_ADMIN} **Management**", value=f"`.checkdeals` **Club Deals**\n`.rc` **Register Club** | `.dc` **Delete Club**\n`.addshopitem` / `.addpokemon` / `.addmysterybox`\n`.addshinycoins` / `.addpc`", inline=False)
            embed.add_field(name=f"{E_AUCTION} **Auctions**", value=f"`.sca` **Club Auction** | `.sda` **Duelist Auction**\n`.fa` **Freeze** | `.ufa` **Unfreeze**", inline=False)
            embed.add_field(name=f"{E_MONEY} **Economy**", value=f"`.tp` **Tip** | `.du` **Deduct** | `.agf` **Group Fund** | `.po` **Payout**", inline=False)

        elif val == "updates":
            embed.title = f"{E_ALERT} Patch Notes v5.8"
            embed.description = f"{E_STARS} **Latest Updates**\n{E_GOLD_TICK} **Interactive Shop:** New .shop menu.\n{E_GOLD_TICK} **User Listings:** Sell Pokémon securely.\n{E_GOLD_TICK} **Tax System:** 5% Tax on User Deals.\n{E_GOLD_TICK} **Approval:** Admin approval for all buys."
            embed.color = 0x9b59b6

        await interaction.response.send_message(embed=embed, ephemeral=True)

class HelpView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(HelpSelect())

@bot.hybrid_command(name="botinfo", aliases=["info", "guide", "help"], description="Open help panel.")
async def botinfo(ctx):
    desc = (
        f"**Welcome to Ze Bot v5.8!**\n"
        f"Created by **Soul Gill**, this bot simulates a high-stakes Football Club Economy and a secure Pokémon Marketplace.\n\n"
        f"**{E_ITEMBOX} NEW: Secure Shop & Inventory**\n"
        f"• **User Market:** List your Pokétwo Pokémon using `/sellpokemon`. Secure transactions with Admin Approval.\n"
        f"• **Admin Shop:** Buy exclusive Items, Pokémon, and Mystery Boxes using {E_SHINY}.\n"
        f"• **Tax System:** Fair 5% tax on User Market deals (2.5% Buyer / 2.5% Seller).\n"
        f"• **Safety:** All deals require Admin Approval. No scams.\n\n"
        f"**{E_FIRE} Football Ecosystem**\n"
        f"• **Clubs:** Buy, auction, and level up football clubs.\n"
        f"• **Duelists:** Register as a player, get signed, earn salary.\n"
        f"• **Groups:** Pool funds with friends to buy massive clubs.\n\n"
        f"**Currencies:**\n"
        f"{E_PC} **Pokécoins:** User Market & Trading.\n"
        f"{E_SHINY} **Shiny Coins:** Admin Shop & Rare Items.\n"
        f"{E_MONEY} **Cash:** Football Clubs & Groups."
    )
    embed = discord.Embed(title=f"{E_CROWN} **Ze Bot System**", description=desc, color=0xf1c40f)
    if bot.user.avatar: embed.set_thumbnail(url=bot.user.avatar.url)
    # UPDATED LINE BELOW:
    embed.add_field(name="Commands", value=f"Use the menu below to browse commands.", inline=False)
    view = HelpView() 
    await ctx.send(embed=embed, view=view)

# ---------- RUN ----------
# ==============================================================================
#  LOGIN REMINDER SYSTEM
# ==============================================================================

# ==============================================================================
#  LOGIN REMINDER SYSTEM
# ==============================================================================

# Define the ID here so it works standalone
LOGIN_LOG_CHANNEL_ID = 1455496870003740736

@bot.command(name="remindlogin", description="Toggle daily login reminders.")
async def remindlogin(ctx):
    uid = str(ctx.author.id)
    
    # 1. Fetch User
    user = wallets_col.find_one({"user_id": uid})
    if not user:
        # Create wallet if doesn't exist so we can save preference
        get_wallet(uid) 
        user = wallets_col.find_one({"user_id": uid})

    # 2. Toggle Status
    current_status = user.get("remind_login", False)
    new_status = not current_status
    
    wallets_col.update_one({"user_id": uid}, {"$set": {"remind_login": new_status}})
    
    # 3. Response
    status_text = "Enabled" if new_status else "Disabled"
    color = 0x2ecc71 if new_status else 0xff0000
    emoji = E_GOLD_TICK if new_status else E_ERROR
    
    embed = create_embed(f"{emoji} Reminder {status_text}", 
                         f"You will {'now' if new_status else 'no longer'} be pinged when your Daily Login is ready.", 
                         color)
    await ctx.send(embed=embed)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    try:
        await bot.tree.sync()
        
        # 1. Register Views (So buttons work after restart)
        bot.add_view(GiveawayView()) 
        bot.add_view(ShopView())
        bot.add_view(BotInfoView())
        
        bot.loop.create_task(pc_claim_alert_task())
        bot.loop.create_task(club_tax_alert_task())
        bot.loop.create_task(check_active_giveaways())# 3. START GIVEAWAY RECOVERY (The Fix)
        bot.add_view(DepositView())

# Add these two new lines!
    bot.add_view(AuctionInfoView(guild_id=824238712770003027)) # Put your actual Server ID here!
    auction_clock.start()
        
        club_market_simulation_task.start()
        
        # 2. Start Market Simulation (If not running)
        if not hasattr(bot, 'market_task_started'):
            bot.loop.create_task(market_simulation_task())
            bot.market_task_started = True

        # 3. Start Login Reminders (If not running)
        if not hasattr(bot, 'reminders_started'):
            # These two lines MUST be indented 4 spaces relative to the 'if' above
            bot.loop.create_task(check_login_reminders())
            bot.reminders_started = True
            
        # 3. START GIVEAWAY RECOVERY (The Fix)
        bot.loop.create_task(check_active_giveaways())
        
    except Exception as e: print(e)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound): pass
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send(embed=create_embed("Error", f"Missing Argument: {error.param}", 0xff0000))
    elif isinstance(error, commands.MissingPermissions): await ctx.send(embed=create_embed("Error", "No Permission.", 0xff0000))
    elif isinstance(error, commands.BadArgument): await ctx.send(embed=create_embed("Error", str(error), 0xff0000))
    else: print(error)

@bot.listen('on_raw_reaction_add')
async def auto_giveaway_quest(payload):
    # Ignore bots
    if payload.member.bot: return
    
    # 1. Check if the emoji used is the giveaway emoji or default popper
    target_emoji_str = str(discord.PartialEmoji.from_str(E_GIVEAWAY)) if E_GIVEAWAY.startswith("<") else "🎉"
    if str(payload.emoji) != target_emoji_str and str(payload.emoji) != "🎉":
        return
        
    # 2. Fetch the channel and message
    channel = bot.get_channel(payload.channel_id)
    if not channel: return
    try:
        msg = await channel.fetch_message(payload.message_id)
    except:
        return
        
    if not msg.embeds: return
    embed = msg.embeds[0]
    
    # 3. Verify it is an official bot giveaway by checking the footer
    if embed.footer and embed.footer.text == "React with 🎉 to enter!":
        uid = str(payload.user_id)
        
        # 4. Check the 24-Hour Cooldown
        w = wallets_col.find_one({"user_id": uid})
        if not w: 
            w = get_wallet(payload.user_id) # Ensure they have a profile
            
        last_react = w.get("last_gw_react")
        now = datetime.now()
        
        if last_react and isinstance(last_react, datetime):
            # If less than 24 hours (86400 seconds) have passed, stop here.
            if (now - last_react).total_seconds() < 86400:
                return 
                
        # 5. Passed cooldown! Update the timestamp and credit the quest
        wallets_col.update_one({"user_id": uid}, {"$set": {"last_gw_react": now}})
        
        # This will update Daily, Weekly, Monthly, Yearly, and Career giveaway quests!
        await update_quest(payload.user_id, "giveaway", 1)
        
        # 6. Send the Premium DM
        try:
            desc = (
                f"Your participation in **{embed.title}** has been verified!\n\n"
                f"{E_GOLD_TICK} **+1 Giveaway Quest Progress** has been added to your profile.\n"
                f"{E_TIMER} You can earn this automatic quest credit again in exactly 24 hours."
            )
            dm_embed = create_embed(f"{E_GIVEAWAY} Quest Completed!", desc, 0x2ecc71)
            await payload.member.send(embed=dm_embed)
        except discord.Forbidden:
            pass # Fails silently if the user has their DMs closed

# ==============================================================================
#  RENDER PORT BINDING (Fix for "No open ports detected")
# ==============================================================================

app = FastAPI()

@app.get("/")
def read_root():
    return {"Status": "Bot is Online!"}

def run_web_server():
    # Render assigns a specific PORT, we must listen to it
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    # 1. Start the fake web server in the background
    t = threading.Thread(target=run_web_server)
    t.start()
    
    # 2. Start the Discord Bot
    bot.run(DISCORD_TOKEN)
































































