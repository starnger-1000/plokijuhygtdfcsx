# bot.py
# Full Club Auction Bot (Blueprint 5.2 - Part 1 of 5)
# PART 1: Imports, Configuration, Assets, Database, and Helper Functions
# Dependencies: discord.py, fastapi, uvicorn, jinja2, pymongo, dnspython, certifi

import os
import asyncio
import random
import re
from datetime import datetime, timedelta
import discord
from discord.ext import commands
from discord.ui import View, Button, Select
from pymongo import MongoClient, ReturnDocument
import certifi

# ==============================================================================
#  1. CONFIGURATION
# ==============================================================================

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

# Owner Configuration
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID")) if os.getenv("BOT_OWNER_ID") else None

# Log Channels (IDs provided in prompts)
LOG_CHANNELS = {
    "withdraw": 1443955732281167873, 
    "battle": 1439844034905374720,
    "club": 1443955856222851142,
    "duelist": 1443955967086690395,
    "shop": 1443955732281167873,     # Admin Shop Sales
    "pending": 1443955732281167873,  # User Market Pending Deals
    "sold_out": 1446017729340379246, # User Market Completed Deals
    "giveaway": 1443955732281167873  # Giveaway Results
}

# Engagement Settings
CHAT_CHANNEL_ID = 975275349573271552 # Channel to track 100 messages
DAILY_MSG_REQ = 100
DAILY_REWARD_AMOUNT = 10000

# Default Image for Donor Giveaways
DONOR_THUMBNAIL_URL = "https://i.imgur.com/YourImageLinkHere.jpg" 

# Game Constants
TIME_LIMIT = 90 
MIN_INCREMENT_PERCENT = 5
LEAVE_PENALTY_PERCENT = 10
DUELIST_MISS_PENALTY_PERCENT = 15
WIN_VALUE_BONUS = 100000
LOSS_VALUE_PENALTY = -100000
OWNER_MSG_VALUE_BONUS = 10000
OWNER_MSG_COUNT_PER_BONUS = 100

# Donor Roles (Role ID: Entry Multiplier)
DONOR_ROLES = {
    972809181444861984: 1, 
    972809182224994354: 1, 
    972809183374225478: 2,
    972809180966703176: 2, 
    972809183718150144: 4, 
    972809184242434048: 8,
    973502021757968414: 12
}

# Club Leveling Configuration
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
#  2. ASSETS (STRICT OG UI & 5.2 UPDATE)
# ==============================================================================

# Core UI Elements
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
E_WINNER_TROPHY = ":918267trophy:"
E_LOSER_MARK = "<a:kids:960920729300377700>"

# 3-Currency System Emojis (Updated 5.2)
E_MONEY = "<a:Donation:962944611792326697>"        # Cash ($)
E_SHINY = "<:pokecoins:1446019648901484616>"       # Shiny Coins (SC)
E_PC = "<a:poke_coin:1446005721370984470>"          # PokeCoins (PC)

BATTLE_BANTER = [
    "<a:redfire1:1443251827490684938> Absolute demolition! **{winner}** tore **{loser}** apart. {l_emoji} <:e:1443996271990935552> What a massacre! {w_emoji}",
    "<a:miapikachu:1443253477533814865> **{winner}** owned the pitch today — sent **{loser}** home with a souvenir bag full of tears. {l_emoji} <:e:1443996214805790871>",
    "<a:cross2:972155180185452544> That wasn’t a match… that was a public execution. RIP **{loser}**. <:e:1443996261941383178> {w_emoji}",
    "<a:red_dot:1443261605092786188> **{loser}** came to play football, **{winner}** came to play career-ending football. {w_emoji}",
    "<a:alert:1443254143308533863> That performance was so one-sided, even the referee felt bad for **{loser}**. <:e:1443996269113643028>",
    "<a:crownop:962190451744579605> **{winner}** delivered a masterclass — **{loser}** just attended the lecture. {w_emoji}"
]

WINNER_REACTIONS = [":7833dakorcalmao:", ":33730ohoholaugh:", ":44158laughs:", ":69692pepewine:", ":954110babythink:"]
LOSER_REACTIONS = ["<:192978sadchinareact:1443996152772038678>", "<:26955despair:1443996205028999360>", "<:8985worldcup:1443996229620203712>"]

# ==============================================================================
#  3. DATABASE CONNECTION
# ==============================================================================

if not MONGO_URL:
    print("CRITICAL: MONGO_URL missing.")
    cluster = None; db = None
else:
    cluster = MongoClient(MONGO_URL, tlsCAFile=certifi.where())
    db = cluster["auction_bot"]

# Initialize Collections
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
    giveaways_col = db.giveaways
    # NEW COLLECTIONS (Blueprint 5.2)
    shop_col = db.shop_items       
    inventory_col = db.user_inventory 
    pokemon_col = db.pokemon_stats 
    market_col = db.user_market    
    daily_stats_col = db.daily_stats 
    coupons_col = db.coupons       
    redeem_codes_col = db.redeem_codes 

def get_next_id(sequence_name):
    if db is None: return 0
    ret = counters_col.find_one_and_update(
        {"_id": sequence_name}, 
        {"$inc": {"seq": 1}}, 
        upsert=True, 
        return_document=ReturnDocument.AFTER
    )
    return ret['seq']

# ==============================================================================
#  4. BOT SETUP & HELPERS
# ==============================================================================

DEFAULT_PREFIX = "."
def get_prefix(bot, message):
    if db is None: return DEFAULT_PREFIX
    try:
        res = config_col.find_one({"key": "prefix"})
        return res["value"] if res else DEFAULT_PREFIX
    except: return DEFAULT_PREFIX

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)
active_timers = {}
bidding_frozen = False

# --- HELPER FUNCTIONS ---

class HumanAmount(commands.Converter):
    """Converts '1k', '1.5m' to integers."""
    async def convert(self, ctx, argument):
        argument = argument.lower().replace(',', '').replace('$', '')
        multiplier = 1
        if argument.endswith('k'): multiplier = 1000; argument = argument[:-1]
        elif argument.endswith('m'): multiplier = 1000000; argument = argument[:-1]
        elif argument.endswith('b'): multiplier = 1000000000; argument = argument[:-1]
        try:
            val = float(argument) * multiplier
            return int(val)
        except ValueError: raise commands.BadArgument(f"Invalid amount: {argument}")

def create_embed(title, description, color=0x2ecc71, thumbnail=None, footer=None):
    embed = discord.Embed(title=title, description=description, color=color)
    if thumbnail: embed.set_thumbnail(url=thumbnail)
    if footer: embed.set_footer(text=footer)
    return embed

def resolve_emoji(item):
    if isinstance(item, int) or (isinstance(item, str) and item.isdigit()):
        e = bot.get_emoji(int(item))
        return str(e) if e else f"<:e:{item}>"
    if isinstance(item, str) and item.startswith(":") and item.endswith(":"):
        e = discord.utils.get(bot.emojis, name=item.strip(":"))
        return str(e) if e else item
    return str(item)

def get_wallet(user_id):
    """Smart Wallet Fetcher (Handles String/Int IDs & Syncs)"""
    if db is None: return None
    # 1. Try Finding by String ID (Standard)
    w = wallets_col.find_one({"user_id": str(user_id)})
    if w: return w
    # 2. Try Finding by Integer ID (Legacy fix)
    w = wallets_col.find_one({"user_id": int(user_id)})
    if w:
        # Auto-Migrate to String for consistency
        wallets_col.update_one({"_id": w["_id"]}, {"$set": {"user_id": str(user_id)}})
        return w
    return None

def log_audit(entry: str):
    if db is not None: audit_col.insert_one({"entry": entry, "timestamp": datetime.now()})

def log_user_activity(user_id, type, description):
    if db is not None: activities_col.insert_one({"user_id": str(user_id), "type": type, "description": description, "timestamp": datetime.now()})

async def send_log(channel_key, embed):
    if db is None: return
    cid = LOG_CHANNELS.get(channel_key)
    if cid:
        ch = bot.get_channel(cid)
        if ch: await ch.send(embed=embed)
# ==============================================================================
#  3. UI CLASSES (VIEWS & DROPDOWNS)
# ==============================================================================

class Paginator(View):
    def __init__(self, ctx, data, title, color, per_page=10):
        super().__init__(timeout=60)
        self.ctx, self.data, self.title, self.color, self.per_page = ctx, data, title, color, per_page
        self.current_page = 0
        self.total_pages = max(1, (len(data) + per_page - 1) // per_page)
        self.update_buttons()

    def update_buttons(self):
        self.children[0].disabled = self.current_page == 0
        self.children[1].disabled = self.current_page == self.total_pages - 1

    def get_embed(self):
        start = self.current_page * self.per_page
        page_data = self.data[start:start + self.per_page]
        embed = discord.Embed(title=f"{self.title} ({self.current_page + 1}/{self.total_pages})", color=self.color)
        for name, value in page_data: embed.add_field(name=name, value=value, inline=False)
        return embed

    @discord.ui.button(emoji="⬅️", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not for you.", ephemeral=True)
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(emoji="➡️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not for you.", ephemeral=True)
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

# --- BOT INFO DROPDOWN ---
class HelpSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Economy", emoji=E_MONEY, description="Wallet, Profile, Withdraw"),
            discord.SelectOption(label="Groups", emoji=E_PREMIUM, description="Create, Join, Deposit, Invest"),
            discord.SelectOption(label="Market & Auctions", emoji=E_AUCTION, description="Bid, Buy, Sell Clubs"),
            discord.SelectOption(label="Duelists", emoji=E_ITEMBOX, description="Register, Salary, Contracts"),
            discord.SelectOption(label="Shop & Items", emoji=E_STARS, description="Buy Items, Shiny Coins, Pokemon"),
            discord.SelectOption(label="Updates", emoji=E_GIVEAWAY, description="Changelog & New Features")
        ]
        super().__init__(placeholder="Select a category...", min_values=1, max_values=1, options=options, custom_id="help_dropdown")

    async def callback(self, interaction: discord.Interaction):
        val = self.values[0]
        embed = None
        
        if val == "Economy":
            desc = f"""
            **{E_MONEY} Economy System**
            Everything revolves around cash. Earn it, save it, or burn it.

            `•` **/profile** (`.p`)
            This is your identity card. It shows your wallet balance, the club you own (if any), the investment groups you belong to, and your recent bidding history.
            *Example:* `.p` or `.p @User`

            `•` **/wallet** (`.bal`)
            A quick way to check your liquid cash balance. This is the money you can use for bidding.
            *Example:* `.bal`

            `•` **/withdrawwallet** (`.ww`)
            **⚠️ WARNING:** This command **BURNS** (deletes) money from your wallet permanently. It is usually used for paying fines or roleplay penalties.
            *Example:* `.ww 50k` (Removes $50,000 forever)
            
            `•` **/daily**
            Claim daily reward. **Requires 100 messages in chat today.**
            *Usage:* `/daily`
            
            `•` **/redeem**
            Redeem a special code for rewards.
            *Example:* `/redeem "WELCOME"`
            """
            embed = create_embed("Economy Guide", desc, 0x3498db)

        elif val == "Groups":
            desc = f"""
            **{E_PREMIUM} Investment Groups**
            Pool your money with friends to buy massive clubs that you can't afford alone.

            `•` **/creategroup**
            Start a new investment group. You can attach an image to the command to set the group's logo immediately.
            *Example:* `/creategroup name:Vikings share:50` (You take 50% equity)

            `•` **/joingroup**
            Join an existing group. You start with 0% equity until you deposit funds or buy shares.
            *Example:* `/joingroup name:Vikings share:0`

            `•` **/groupinfo** (`.gi`)
            View detailed stats about a group: Bank balance, member list with share %, and owned clubs.
            *Example:* `.gi Vikings`

            `•` **/deposit** (`.dep`)
            Transfer funds from your personal wallet to the group's bank. This increases the group's buying power.
            *Example:* `.dep Vikings 1.5m`

            `•` **/withdraw** (`.wd`)
            Withdraw funds from the group bank to your personal wallet.
            *Example:* `.wd Vikings 500k`

            `•` **/leavegroup**
            Leave a group permanently. **Warning:** This may incur a 10% penalty fee deducted from the group funds.
            *Example:* `.leavegroup Vikings`
            """
            embed = create_embed("Group System Guide", desc, 0x9b59b6)

        elif val == "Market & Auctions":
            desc = f"""
            **{E_AUCTION} Market & Auctions**
            The core of the game. Buy clubs, trade shares, and win wars.

            `•` **/placebid** (`.pb`)
            Place a bid on a Club or Duelist. If you bid within the last few seconds, the timer resets to 90s.
            *Example:* `.pb 1.5m club 10` (Bids $1.5 Million on Club ID 10)

            `•` **/groupbid** (`.gb`)
            Place a bid using your Group's funds. You must be a member of the group to do this.
            *Example:* `.gb Vikings 5m club 10`

            `•` **/sellclub** (`.sc`)
            Sell your club. You can sell it to the open market (set price) or directly to another user.
            *Example:* `.sc "Real Madrid" @User`

            `•` **/marketbuy** (`.mb`)
            Instantly buy an unowned club from the market list at its current value. No auction required.
            *Example:* `.mb 15` (Buys Club ID 15 instantly)

            `•` **/listclubs** (`.lc`)
            View a paginated list of all registered clubs in the server.
            *Example:* `.lc`
            
            `•` **/marketlist** (`.ml`)
            View a specific list of **Unowned** clubs that are available for instant purchase.
            *Example:* `.ml`
            """
            embed = create_embed("Market Guide", desc, 0xe67e22)

        elif val == "Duelists":
            desc = f"""
            **{E_ITEMBOX} Duelist Career System**
            Sign up as a player, get auctioned, and earn a salary.

            `•` **/registerduelist** (`.rd`)
            Register yourself as a player available for auction. You set your base price and expected salary.
            *Example:* `.rd "Striker99" 50000 10000`

            `•` **/listduelists** (`.ld`)
            View all registered players, their ID, and their current team (if any).
            *Example:* `.ld`

            `•` **/retireduelist**
            Delete your duelist profile. If you are currently signed to a club, the owner must approve this.
            *Example:* `.retireduelist`

            `•` **/adjustsalary** (`.adj`)
            **(Owner Only)** Pay a bonus to a player (positive amount) or issue a fine (negative amount).
            *Example (Bonus):* `.adj 5 50k` (Pays Player ID 5 $50k)
            *Example (Fine):* `.adj 5 -10k` (Deducts $10k from Player ID 5)

            `•` **/deductsalary**
            **(Owner Only)** Automatically deducts a 15% standard penalty from a player's wallet for missing a match.
            *Example:* `.deductsalary 5 yes`
            """
            embed = create_embed("Duelist System Guide", desc, 0xf1c40f)
        
        elif val == "Shop & Items":
            desc = f"""
            **{E_STARS} Shop & Inventory System**
            Buy rare items, shiny coins, or entire Pokemon.

            `•` **/shop**
            Open the interactive shop menu. Select a category (Items, Shiny Coins, Pokemon, User Market) to view stock.
            *Usage:* `/shop`

            `•` **/buy**
            Purchase an item from the shop or User Market.
            *Example:* `/buy "Master Ball" 5`
            *Example:* `/buy U5` (Buys Pokemon ID U5 from user market)

            `•` **/listitem** (or `/sellpokemon`)
            Sell a Pokemon from your inventory to the **User Market** for PC. Requires Poketwo Info embed.
            *Example:* `/sellpokemon 5000` (Sells for 5,000 PC)
            
            `•` **/depositpc** (`.dpc`)
            Sync your Poketwo PC balance to the bot to buy from User Market.
            *Usage:* `.dpc` (Then follow instructions)

            `•` **/inventory** (`.inv`)
            View your purchased items and shiny coin balance.
            *Usage:* `.inv`
            
            `•` **/pinfo**
            View detailed stats of a Pokemon (mimics Poketwo style) before buying.
            *Example:* `/pinfo U5` (Views Pokemon ID U5 from shop)
            
            `•` **/use**
            Open a Mystery Box.
            *Example:* `/use "Shiny Mystery Box"`
            """
            embed = create_embed("Shop System", desc, 0xFF69B4)

        elif val == "Updates":
            desc = f"""
            **{E_GIVEAWAY} Recent Major Updates (v5.2)**
            
            **1. Smart Syntax & Aliases**
            You no longer need to type long zeros!
            • Use `k` for thousands (`50k` = 50,000)
            • Use `m` for millions (`1.5m` = 1,500,000)
            • Use shortcuts like `.pb`, `.gb`, `.dep`, `.wd`, `.ci`, `.gi`

            **2. Advanced Giveaways**
            • **/gstart_donor:** Special giveaways that check for Donor roles and give weighted entries (e.g., 12x entries for top donors).
            • **/gstart_daily:** Simple public giveaways for everyone.
            • **Auto-Payout:** If the prize is money (e.g., "500k"), the bot automatically adds it to the winner's wallet!
            • **Images:** You can now attach an image file when starting a giveaway.

            **3. Market Overhaul**
            • **Sold Out Logic:** You cannot bid on a club if it is already owned by someone else (unless they sell it).
            • **One Club Rule:** A user or group can only hold 100% ownership of ONE club at a time.
            • **Instant Buy:** Added `/marketbuy` to snatch up unowned clubs immediately.
            
            **4. Shop & User Market**
            • **3 Currencies:** Cash ($), Shiny Coins ({E_SHINY}), Pokécoins ({E_PC}).
            • **User Market:** P2P trading with 5% Tax and Admin Approval.
            • **Mystery Boxes:** Category-based boxes (Shiny/Rare/Regional).
            """
            embed = create_embed("Changelog", desc, 0x2ecc71)

        await interaction.response.send_message(embed=embed, ephemeral=True)

class BotInfoView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(HelpSelect())

# --- SHOP DROPDOWN ---
class ShopSelect(Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Items", emoji=E_ITEMBOX, description="Balls, Potions, etc."),
            discord.SelectOption(label="Shiny Coins", emoji=E_MONEY, description="Buy Shiny Coins"),
            discord.SelectOption(label="Pokemon", emoji=E_PIKACHU, description="Buy Rare Pokemon"),
            discord.SelectOption(label="Mystery Boxes", emoji=E_GIVEAWAY, description="Buy Mystery Boxes"),
            discord.SelectOption(label="User Market", emoji=E_AUCTION, description="Buy from other players"),
        ]
        super().__init__(placeholder="Select Shop Category...", min_values=1, max_values=1, options=options, custom_id="shop_dropdown")
    
    async def callback(self, interaction):
        val = self.values[0]
        items = []
        if val == "User Market":
             items = list(market_col.find({"status": "active"}))
        else:
             items = list(shop_col.find({"category": val}))
        
        if not items:
            return await interaction.response.send_message(embed=create_embed(f"{val} Shop", "Sold out / Empty.", 0x95a5a6), ephemeral=True)
        
        desc = ""
        currency = E_PC if val == "User Market" else E_SHINY
        for item in items:
            price = f"{currency} {item['price']:,}"
            stock = item.get('stock', '∞')
            seller = f"Seller: <@{item['seller_id']}>" if val == "User Market" else "Admin"
            item_id_display = f"ID: `{item['id']}`"
            
            desc += f"**{item['name']}** ({item_id_display})\nPrice: {price} | Stock: {stock} | {seller}\n\n"
        
        await interaction.response.send_message(embed=create_embed(f"{E_STARS} {val} Shop", desc, 0xFF69B4), ephemeral=True)

class ShopView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ShopSelect())

class GiveawayView(View):
    def __init__(self, giveaway_id=None, required_role_id=None):
        super().__init__(timeout=None) 
        self.giveaway_id = giveaway_id
        self.required_role_id = required_role_id

    @discord.ui.button(label="React to Enter", emoji=E_GIVEAWAY, style=discord.ButtonStyle.success, custom_id="gw_join")
    async def join_button(self, interaction, button):
        if db is None: return
        gw = giveaways_col.find_one({"message_id": interaction.message.id})
        if not gw or gw.get("ended"): return await interaction.response.send_message(embed=create_embed("Error", "❌ Ended.", 0xff0000), ephemeral=True)

        if gw.get("type") == "req":
            role = interaction.guild.get_role(int(gw["required_role_id"]))
            if role and role not in interaction.user.roles: return await interaction.response.send_message(embed=create_embed("Req", f"Missing {role.mention}", 0xff0000), ephemeral=True)
        
        entries = 1
        if gw.get("type") == "donor":
            user_roles = [r.id for r in interaction.user.roles]
            has_donor = False
            for rid, mul in DONOR_ROLES.items():
                if rid in user_roles: 
                    has_donor = True
                    if mul > entries: entries = mul
            if not has_donor: return await interaction.response.send_message(embed=create_embed("Req", "❌ Donor Only.", 0xff0000), ephemeral=True)

        if giveaways_col.find_one({"message_id": interaction.message.id, "participants.user_id": interaction.user.id}):
            return await interaction.response.send_message(embed=create_embed("Info", "⚠️ Joined.", 0x95a5a6), ephemeral=True)
        
        giveaways_col.update_one({"message_id": interaction.message.id}, {"$push": {"participants": {"user_id": interaction.user.id, "entries": entries}}})
        await interaction.response.send_message(embed=create_embed("Success", f"✅ Joined! ({entries}x)", 0x2ecc71), ephemeral=True)

    @discord.ui.button(label="List", emoji="📋", style=discord.ButtonStyle.secondary, custom_id="gw_list")
    async def list_button(self, interaction, button):
        if not interaction.user.guild_permissions.administrator: return
        gw = giveaways_col.find_one({"message_id": interaction.message.id})
        parts = gw.get("participants", []) if gw else []
        txt = "\n".join([f"<@{p['user_id']}> ({p['entries']}x)" for p in parts[:20]])
        if len(parts) > 20: txt += f"\n...and {len(parts)-20} more."
        await interaction.response.send_message(embed=create_embed("Participants", txt or "None", 0x3498db), ephemeral=True)

class MarketConfirmView(View):
    def __init__(self, ctx, club_id, price, buyer_id, is_group=False):
        super().__init__(timeout=60)
        self.ctx, self.club_id, self.price, self.buyer_id, self.is_group = ctx, club_id, price, buyer_id, is_group
        self.value = None

    @discord.ui.button(label="Confirm Buy", style=discord.ButtonStyle.success, custom_id="market_confirm")
    async def confirm(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return
        
        if self.is_group:
            g = groups_col.find_one({"name": self.buyer_id.replace("group:", "")})
            bal = g["funds"]
        else:
            w = get_wallet(self.buyer_id)
            bal = w["balance"] if w else 0
            
        if bal < self.price: return await interaction.response.send_message(embed=create_embed("Error", "Insufficient funds.", 0xff0000), ephemeral=True)

        if self.is_group: groups_col.update_one({"name": self.buyer_id.replace("group:", "")}, {"$inc": {"funds": -self.price}})
        else: wallets_col.update_one({"user_id": str(self.buyer_id)}, {"$inc": {"balance": -self.price}})

        clubs_col.update_one({"id": self.club_id}, {"$set": {"owner_id": str(self.buyer_id), "last_bid_price": self.price}})
        if not self.is_group:
            profiles_col.update_one({"user_id": str(self.buyer_id)}, {"$set": {"owned_club_id": self.club_id}}, upsert=True)

        await interaction.response.send_message(embed=create_embed(f"{E_SUCCESS} Purchased!", f"Club ID {self.club_id} bought for ${self.price:,}", 0x2ecc71))
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="market_cancel")
    async def cancel(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return
        await interaction.response.send_message("Cancelled.", ephemeral=True)
        self.stop()

# ==============================================================================
#  CORE LOGIC FUNCTIONS (Auctions, Giveaways, Levels)
# ==============================================================================

def get_club_owner_info(club_id):
    if db is None: return None, []
    c = clubs_col.find_one({"id": int(club_id)})
    if not c or not c.get("owner_id"): return None, []
    owner = c["owner_id"]
    if owner.startswith('group:'):
        mems = group_members_col.find({"group_name": owner.replace('group:', '').lower()})
        return owner, [m['user_id'] for m in mems]
    return owner, [owner]

def min_required_bid(current):
    return int(current + max(1, round(current * MIN_INCREMENT_PERCENT / 100)))

def get_level_info(wins):
    lvl, nxt, req = LEVEL_UP_CONFIG[0][1], None, 0
    for w, n, b in LEVEL_UP_CONFIG:
        if w > wins: nxt, req = (n, w, b), w - wins; break
        elif w <= wins: lvl = n
    return lvl, nxt, req

def update_club_level(club_id, wins_gained=0):
    if db is None: return
    c = clubs_col.find_one({"id": int(club_id)})
    if not c: return
    new_wins = c.get("total_wins", 0) + wins_gained
    clubs_col.update_one({"id": int(club_id)}, {"$set": {"total_wins": new_wins}})
    for w, n, b in LEVEL_UP_CONFIG:
        if c.get("total_wins", 0) < w <= new_wins:
            clubs_col.update_one({"id": int(club_id)}, {"$set": {"level_name": n}, "$inc": {"value": b}})
            
async def finalize_auction(item_type, item_id, channel_id):
    if db is None: return
    bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
    ch = bot.get_channel(channel_id)
    item = clubs_col.find_one({"id": int(item_id)}) if item_type == "club" else duelists_col.find_one({"id": int(item_id)})
    
    if bid:
        bidder, amt = bid["bidder"], int(bid["amount"])
        if bidder.startswith('group:'): 
            groups_col.update_one({"name": bidder.replace('group:', '').lower()}, {"$inc": {"funds": -amt}})
        else: 
            wallets_col.update_one({"user_id": bidder}, {"$inc": {"balance": -amt}})
        
        if item_type == "club":
            old = item.get("owner_id")
            if old and not old.startswith("group:"): 
                profiles_col.update_one({"user_id": old}, {"$unset": {"owned_club_id": ""}})
            history_col.insert_one({"club_id": int(item_id), "winner": bidder, "amount": amt, "timestamp": datetime.now()})
            clubs_col.update_one({"id": int(item_id)}, {"$set": {"owner_id": bidder, "value": amt, "ex_owner_id": old}})
            if not bidder.startswith('group:'): profiles_col.update_one({"user_id": bidder}, {"$set": {"owned_club_id": int(item_id)}}, upsert=True)
            
            embed = create_embed(f"{E_GIVEAWAY} SOLD!", f"{E_SUCCESS} **Owner:** {bidder}\n{E_MONEY} **Price:** ${amt:,}", 0xf1c40f, item.get('logo'))
            if ch: await ch.send(embed=embed)
            await send_log("club", embed)
        else:
            contracts_col.insert_one({"duelist_id": int(item_id), "club_owner": bidder, "amount": amt, "timestamp": datetime.now()})
            cid = None
            if bidder.startswith('group:'): 
                c = clubs_col.find_one({"owner_id": bidder})
                if c: cid = c['id']
            else: 
                c = clubs_col.find_one({"owner_id": bidder})
                if c: cid = c['id']
            duelists_col.update_one({"id": int(item_id)}, {"$set": {"owned_by": bidder, "club_id": cid}})
            wallets_col.update_one({"user_id": item["discord_user_id"]}, {"$inc": {"balance": amt}}, upsert=True)
            
            embed = create_embed(f"{E_GIVEAWAY} SIGNED!", f"{E_SUCCESS} **To:** {bidder}\n{E_MONEY} **Fee:** ${amt:,} (Paid to Player)", 0x9b59b6, item.get('avatar_url'))
            if ch: await ch.send(embed=embed)
            await send_log("duelist", embed)
    else:
        if ch: await ch.send(embed=create_embed(f"{E_TIMER} Ended", "No bids received.", 0x95a5a6))
    
    bids_col.delete_many({"item_type": item_type, "item_id": int(item_id)})
    active_timers.pop((item_type, str(item_id)), None)

def schedule_auction_timer(item_type, item_id, channel_id):
    key = (item_type, str(item_id))
    if active_timers.get(key) and not active_timers.get(key).done(): active_timers[key].cancel()
    loop = asyncio.get_event_loop()
    t = loop.create_task(asyncio.sleep(TIME_LIMIT))
    async def wrapper():
        try: await t; await finalize_auction(item_type, item_id, channel_id)
        except asyncio.CancelledError: return
    active_timers[key] = loop.create_task(wrapper())

def parse_prize_amount(prize_str):
    prize_str = prize_str.lower().replace(',', '')
    mul = 1
    if 'k' in prize_str: mul = 1000; prize_str = prize_str.replace('k', '')
    elif 'm' in prize_str: mul = 1000000; prize_str = prize_str.replace('m', '')
    try: return int(float(prize_str) * mul)
    except: return None

async def end_giveaway(mid, ch, prize):
    if db is None: return
    gw = giveaways_col.find_one({"message_id": mid})
    if not gw or gw['ended']: return
    parts = gw.get('participants', [])
    if not parts:
        await ch.send(embed=create_embed("Ended", "No participants joined.", 0x95a5a6))
        return giveaways_col.update_one({"message_id": mid}, {"$set": {"ended": True}})
    
    pool = []
    for p in parts: pool.extend([p['user_id']] * p['entries'])
    winner_id = random.choice(pool)
    giveaways_col.update_one({"message_id": mid}, {"$set": {"ended": True, "winner_id": winner_id}})
    
    amt = parse_prize_amount(prize)
    msg = ""
    if amt:
        wallets_col.update_one({"user_id": str(winner_id)}, {"$inc": {"balance": amt}}, upsert=True)
        msg = f"\n{E_MONEY} **Auto-Tipped:** ${amt:,}"
    
    await ch.send(f"<@{winner_id}>", embed=create_embed(f"{E_GIVEAWAY} WINNER!", f"Congratulations <@{winner_id}>!\nYou won **{prize}**!{msg}", 0xf1c40f))
# ==============================================================================
#  GROUP 5: DUELIST COMMANDS
# ==============================================================================

@bot.hybrid_command(name="registerduelist", aliases=["rd"], description="Register as a duelist.")
async def registerduelist(ctx, username: str, base_price: HumanAmount, salary: HumanAmount):
    if duelists_col.find_one({"discord_user_id": str(ctx.author.id)}): 
        return await ctx.send(embed=create_embed("Error", "Already registered.", 0xff0000))
    
    did = get_next_id("duelist_id")
    avatar = ctx.author.avatar.url if ctx.author.avatar else ""
    
    duelists_col.insert_one({
        "id": did, 
        "discord_user_id": str(ctx.author.id), 
        "username": username, 
        "base_price": base_price, 
        "expected_salary": salary, 
        "avatar_url": avatar, 
        "owned_by": None, 
        "club_id": None
    })
    
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Registered", f"Duelist **{username}** (ID: {did})", 0x9b59b6))

@bot.hybrid_command(name="listduelists", aliases=["ld"], description="List all registered duelists.")
async def listduelists(ctx):
    ds = list(duelists_col.find())
    data = []
    for d in ds:
        cname = "Free Agent"
        if d.get("club_id"):
            c = clubs_col.find_one({"id": d["club_id"]})
            if c: cname = c["name"]
        data.append((f"{d['username']}", f"{E_ITEMBOX} ID: {d['id']}\n{E_MONEY} ${d['expected_salary']:,}\n{E_STAR} {cname}"))
    
    view = Paginator(ctx, data, f"{E_ITEMBOX} Duelist Registry", 0x9b59b6)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="retireduelist", aliases=["ret"], description="Retire your duelist profile.")
async def retireduelist(ctx):
    d = duelists_col.find_one({"discord_user_id": str(ctx.author.id)})
    if not d: return await ctx.send("Not a duelist.")
    
    if d.get("club_id"):
         # In a real scenario, you might want to alert the club owner here
         pass
        
    duelists_col.delete_one({"_id": d["_id"]})
    await ctx.send(embed=create_embed(f"{E_DANGER} Retired", "Duelist profile deleted.", 0xff0000))

@bot.hybrid_command(name="adjustsalary", aliases=["as"], description="Owner: Bonus or Fine.")
async def adjustsalary(ctx, duelist_id: int, amount: HumanAmount):
    d = duelists_col.find_one({"id": duelist_id})
    if not d or not d.get("club_id"): return await ctx.send("Invalid duelist.")
    
    owner_str, owner_ids = get_club_owner_info(d["club_id"])
    if str(ctx.author.id) not in owner_ids: return await ctx.send("Not owner.")

    # Transaction
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
    
    action = "Bonus" if amount > 0 else "Fine"
    await ctx.send(embed=create_embed(f"{E_MONEY} Salary Adjusted", f"{action}: ${abs(amount):,}", 0x2ecc71))

@bot.hybrid_command(name="deductsalary", aliases=["ds"], description="Owner: Match Penalty (15%).")
async def deductsalary(ctx, duelist_id: int, confirm: str):
    if confirm.lower() != "yes": return
    d = duelists_col.find_one({"id": duelist_id})
    if not d: return
    
    penalty = int(d["expected_salary"] * (DUELIST_MISS_PENALTY_PERCENT / 100))
    
    wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": -penalty}}, upsert=True)
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": penalty}}, upsert=True)
    
    log_user_activity(d["discord_user_id"], "Penalty", f"Fined ${penalty:,} for missed match.")
    await ctx.send(embed=create_embed(f"{E_ALERT} Penalty Applied", f"Deducted **${penalty:,}** (15%)", 0xff0000))

# ==============================================================================
#  GROUP 6: SHOP, INVENTORY & MYSTERY BOX
# ==============================================================================

@bot.hybrid_command(name="shop", description="Open the Server Shop.")
async def shop(ctx):
    embed = create_embed(f"{E_STARS} Server Marketplace", "Select a category below to browse items, pokemon, or the user market.", 0xFF69B4)
    view = ShopView()
    await ctx.send(embed=embed, view=view)

@bot.hybrid_command(name="buycoins", aliases=["bc"], description="Convert Cash to Shiny Coins ($100 = 1 SC).")
async def buycoins(ctx, amount: int):
    cost = amount * 100
    w = get_wallet(ctx.author.id)
    if not w or w.get("balance", 0) < cost: return await ctx.send(embed=create_embed("Error", f"Need ${cost:,} Cash.", 0xff0000))
    
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -cost, "shiny_coins": amount}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Exchange", f"Bought **{amount:,} SC** for **${cost:,}**.", 0x2ecc71))

@bot.hybrid_command(name="depositpc", aliases=["dpc"], description="Sync your Poketwo PC balance.")
async def depositpc(ctx):
    embed = create_embed(
        f"{E_PC} Deposit / Sync Balance",
        f"To update your **Pokécoin (PC)** wallet, I need to verify your balance.\n"
        f"Please run the following command **in this channel** now:\n\n"
        f"`<@716390085896962058> bal`\n\n"
        f"*{E_ACTIVE} I will automatically detect the response.*",
        0x3498db
    )
    await ctx.send(embed=embed)

@bot.hybrid_command(name="buy", description="Buy Item/Pokemon (Admin SC / User PC).")
async def buy(ctx, item_id: str, quantity: int = 1, coupon: str = None):
    # --- USER MARKET LOGIC (Starts with U) ---
    if item_id.startswith("U"):
        listing = market_col.find_one({"id": item_id})
        if not listing: return await ctx.send("Listing not found.")
        if listing.get("status") == "pending": return await ctx.send("Item is pending approval.")
        
        price = listing['price']
        tax = int(price * 0.025) # 2.5% Buyer Tax
        total_cost = price + tax
        
        w = get_wallet(ctx.author.id)
        if not w or w.get("pc", 0) < total_cost: return await ctx.send(embed=create_embed("Error", f"Insufficient PC. Need {total_cost:,} PC.", 0xff0000))
        
        # Deduct & Hold Funds
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"pc": -total_cost}})
        market_col.update_one({"id": item_id}, {"$set": {"status": "pending", "buyer_id": str(ctx.author.id)}})
        
        # Send Pending Deal to Admins
        chn = bot.get_channel(LOG_CHANNELS["pending"]) 
        if chn:
            # Simple Approval View
            view = View(timeout=None)
            
            async def approve_cb(interaction):
                if interaction.user.id != interaction.message.author.id and not interaction.user.guild_permissions.administrator: return
                seller_id = listing['seller_id']
                seller_get = int(price - (price * 0.025)) # Seller pays 2.5% tax
                wallets_col.update_one({"user_id": seller_id}, {"$inc": {"pc": seller_get}})
                
                # Add to Buyer Inventory (Pokemon Data)
                pokemon_col.update_one({"id": listing['p_id']}, {"$set": {"owner_id": str(ctx.author.id)}})
                inventory_col.update_one({"user_id": str(ctx.author.id), "item_id": listing['p_id']}, {"$inc": {"quantity": 1}, "$set": {"name": listing['name'], "type": "Pokemon"}}, upsert=True)
                market_col.delete_one({"id": item_id})
                
                await interaction.message.delete()
                await interaction.channel.send(embed=create_embed(f"{E_SUCCESS} Approved", f"Deal {item_id} completed.", 0x2ecc71))
                
                # Log Sold
                s_ch = bot.get_channel(LOG_CHANNELS["sold_out"])
                if s_ch: await s_ch.send(embed=create_embed(f"{E_GIVEAWAY} SOLD!", f"**{listing['name']}** sold to <@{ctx.author.id}> for {total_cost:,} PC.", 0xf1c40f))

            async def premium_approve_cb(interaction):
                if interaction.user.id != interaction.message.author.id and not interaction.user.guild_permissions.administrator: return
                # Refund Tax to Buyer, Full Price to Seller
                wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"pc": tax}}) # Refund tax
                wallets_col.update_one({"user_id": listing['seller_id']}, {"$inc": {"pc": price}}) # Full price
                
                # Transfer
                inventory_col.update_one({"user_id": str(ctx.author.id), "item_id": listing['p_id']}, {"$inc": {"quantity": 1}, "$set": {"name": listing['name'], "type": "Pokemon"}}, upsert=True)
                market_col.delete_one({"id": item_id})
                
                await interaction.message.delete()
                await interaction.channel.send(embed=create_embed(f"{E_STARS} Premium Approved", "Tax waived.", 0x2ecc71))

            async def decline_cb(interaction):
                if interaction.user.id != interaction.message.author.id and not interaction.user.guild_permissions.administrator: return
                # Refund Buyer Full
                wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"pc": total_cost}})
                market_col.update_one({"id": item_id}, {"$set": {"status": "active", "buyer_id": None}})
                await interaction.message.delete()
                await interaction.channel.send(embed=create_embed(f"{E_ERROR} Declined", "Money refunded.", 0xff0000))

            b1 = Button(label="Approve", style=discord.ButtonStyle.green, custom_id=f"approve_{item_id}"); b1.callback = approve_cb
            b2 = Button(label="No Tax (Premium)", style=discord.ButtonStyle.blurple, custom_id=f"premium_{item_id}"); b2.callback = premium_approve_cb
            b3 = Button(label="Decline", style=discord.ButtonStyle.red, custom_id=f"decline_{item_id}"); b3.callback = decline_cb
            view.add_item(b1); view.add_item(b2); view.add_item(b3)
            
            await chn.send(embed=create_embed(f"{E_ALERT} Pending Deal: {item_id}", f"Item: {listing['name']}\nBuyer: {ctx.author.mention}\nPrice: {total_cost:,} PC", 0xe67e22), view=view)
        
        await ctx.send(embed=create_embed(f"{E_TIMER} Pending", "Deal sent for approval. PC deducted.", 0xe67e22))

    # --- ADMIN SHOP LOGIC (Normal ID) - Uses SC ---
    else:
        query = {"id": int(item_id)} if item_id.isdigit() else {"id": item_id}
        item = shop_col.find_one(query)
        if not item: return await ctx.send("Item not found.")
        
        price = item['price'] * quantity
        
        # Coupon Logic
        if coupon:
            cp = coupons_col.find_one({"code": coupon})
            if cp and cp['uses'] < cp['max_uses']:
                price = int(price * (1 - (cp['discount']/100)))
                coupons_col.update_one({"_id": cp['_id']}, {"$inc": {"uses": 1}})
            else: return await ctx.send("Invalid coupon.")

        w = get_wallet(ctx.author.id)
        if not w or w.get("shiny_coins", 0) < price: return await ctx.send(embed=create_embed("Error", f"Insufficient SC. Need {price:,} SC.", 0xff0000))
        
        if item['stock'] != -1 and item['stock'] < quantity: return await ctx.send("Out of stock.")
        
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"shiny_coins": -price}})
        if item['stock'] != -1: shop_col.update_one({"_id": item['_id']}, {"$inc": {"stock": -quantity}})
        
        # Add to Inventory
        inventory_col.update_one({"user_id": str(ctx.author.id), "item_id": item['id']}, {"$inc": {"quantity": quantity}, "$set": {"name": item['name'], "type": item['category']}}, upsert=True)
        
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Purchased", f"Bought {quantity}x {item['name']} for {price:,} SC.", 0x2ecc71))

@bot.hybrid_command(name="use", description="Open a Mystery Box.")
async def use(ctx, box_name: str):
    # Check Inventory
    inv = inventory_col.find_one({"user_id": str(ctx.author.id), "name": {"$regex": f"^{box_name}", "$options": "i"}})
    if not inv or inv['quantity'] < 1: return await ctx.send("You don't have this box.")
    
    # Determine Rarity Tag based on box name
    tag = "Common"
    if "shiny" in box_name.lower(): tag = "Shiny"
    elif "rare" in box_name.lower(): tag = "Rare"
    elif "regional" in box_name.lower(): tag = "Regional"
    
    # Find Prize in Admin Shop (Category: Pokemon, Tag: Matches)
    prizes = list(shop_col.find({"category": "Pokemon", "tags": tag}))
    if not prizes: return await ctx.send(f"No {tag} Pokemon currently in stock.")
    
    win = random.choice(prizes)
    
    # Transaction
    inventory_col.update_one({"_id": inv["_id"]}, {"$inc": {"quantity": -1}}) # Remove Box
    shop_col.delete_one({"_id": win["_id"]}) # Remove from Shop (One time use)
    
    # Give Pokemon
    inventory_col.update_one({"user_id": str(ctx.author.id), "item_id": win['id']}, {"$inc": {"quantity": 1}, "$set": {"name": win['name'], "type": "Pokemon"}}, upsert=True)
    
    # Fetch Image for Embed
    p_stats = pokemon_col.find_one({"id": win['id']})
    img = p_stats['image_url'] if p_stats else None
    
    await ctx.send(embed=create_embed(f"{E_GIVEAWAY} BOX OPENED!", f"You found: **{win['name']}**!", 0xFF69B4, img))

@bot.hybrid_command(name="sellpokemon", aliases=["sellp", "listitem"], description="List a Pokemon on User Market (PC).")
async def sellpokemon(ctx, price: int):
    await ctx.send(embed=create_embed(f"{E_AUCTION} Listing...", f"Please run `<@716390085896962058> info` now to verify stats.", 0xe67e22))
    
    def check(m): return m.channel == ctx.channel and m.author.id == 716390085896962058 and m.embeds
    
    try:
        msg = await bot.wait_for('message', check=check, timeout=30.0)
        emb = msg.embeds[0]
        
        # Scraping Logic
        name = emb.title or "Unknown Pokemon"
        img = emb.thumbnail.url if emb.thumbnail else ""
        desc = emb.description or ""
        
        # Auto-Tagging
        tags = []
        if "✨" in name: tags.append("Shiny")
        if "Legendary" in desc: tags.append("Rare")
        if "Fire" in desc: tags.append("Fire")
        
        uid = f"U{get_next_id('market_id')}"
        
        # Create Listing
        market_col.insert_one({
            "id": uid, "p_id": f"P{get_next_id('poke_data_id')}", "name": name, 
            "price": price, "seller_id": str(ctx.author.id), "tags": tags, 
            "status": "active", "buyer_id": None
        })
        
        # Save Stats for /pinfo
        pokemon_col.insert_one({"id": uid, "name": name, "image_url": img, "raw_data": desc})
        
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Listed!", f"**{name}** listed as `{uid}` for {price:,} PC.", 0x2ecc71, img))
        
    except asyncio.TimeoutError:
        await ctx.send("Timed out. Listing cancelled.")

@bot.hybrid_command(name="marketsearch", aliases=["ms"], description="Filter User Market.")
async def marketsearch(ctx, category: str):
    items = list(market_col.find({"tags": category, "status": "active"}))
    if not items: return await ctx.send("No items found.")
    
    desc = ""
    for i in items: desc += f"**{i['name']}** (ID: {i['id']}) - {i['price']:,} PC\n"
    
    await ctx.send(embed=create_embed(f"Search: {category}", desc, 0xe67e22))

@bot.hybrid_command(name="inventory", aliases=["inv"], description="View inventory.")
async def inventory(ctx):
    items = list(inventory_col.find({"user_id": str(ctx.author.id), "quantity": {"$gt": 0}}))
    w = get_wallet(ctx.author.id)
    sc = w.get("shiny_coins", 0) if w else 0
    
    desc = f"{E_SHINY} **Shiny Coins:** {sc:,}\n\n**Items:**\n"
    for i in items:
        desc += f"• {i['name']} (x{i['quantity']})\n"
        
    await ctx.send(embed=create_embed(f"{E_ITEMBOX} Inventory", desc, 0x3498db))

@bot.hybrid_command(name="pinfo", description="View Pokemon stats.")
async def pinfo(ctx, shop_id: str):
    # Check Shop, Market, or Pokemon Stats
    p = pokemon_col.find_one({"id": shop_id})
    if not p: return await ctx.send("Pokemon data not found.")
    
    embed = create_embed(p.get('name'), p.get('raw_data', "No data"), 0x3498db, p.get('image_url'))
    await ctx.send(embed=embed)
# ==============================================================================
#  ADMIN COMMANDS (Shop & General)
# ==============================================================================

@bot.hybrid_command(name="addshopitem", aliases=["asi"], description="Admin: Add item.")
@commands.has_permissions(administrator=True)
async def addshopitem(ctx, name: str, category: str, price: int):
    sid = f"A{get_next_id('shop_id')}"
    shop_col.insert_one({"id": sid, "name": name, "category": category, "price": price, "stock": -1})
    await ctx.send(f"Added {name} ({sid}).")

@bot.hybrid_command(name="addpokemon", aliases=["ap"], description="Admin: Add Pokemon (Scrape).")
@commands.has_permissions(administrator=True)
async def addpokemon(ctx, price: int, category: str, tag: str):
    await ctx.send("Show Info now.")
    def check(m): return m.channel == ctx.channel and m.author.id == 716390085896962058 and m.embeds
    try:
        msg = await bot.wait_for('message', check=check, timeout=30.0)
        emb = msg.embeds[0]
        name = emb.title
        img = emb.thumbnail.url
        
        sid = f"A{get_next_id('shop_id')}"
        shop_col.insert_one({"id": sid, "name": name, "category": "Pokemon", "price": price, "tags": tag, "stock": 1})
        pokemon_col.insert_one({"id": sid, "name": name, "image_url": img, "raw_data": emb.description})
        
        await ctx.send(f"Added {name} as {sid} (Tag: {tag})")
    except: await ctx.send("Timeout.")

@bot.hybrid_command(name="createcoupon", description="Admin: Create Coupon.")
@commands.has_permissions(administrator=True)
async def createcoupon(ctx, code: str, discount: int, uses: int):
    coupons_col.insert_one({"code": code, "discount": discount, "uses": 0, "max_uses": uses})
    await ctx.send(f"Coupon {code} created ({discount}% off).")

@bot.hybrid_command(name="createcode", description="Admin: Create Redeem Code.")
@commands.has_permissions(administrator=True)
async def createcode(ctx, code: str, type: str, amount: int, uses: int):
    redeem_codes_col.insert_one({"code": code, "type": type, "amount": amount, "uses": 0, "max_uses": uses, "claimed_by": []})
    await ctx.send(f"Code {code} created.")

@bot.hybrid_command(name="addmysterybox", aliases=["amb"], description="Admin: Add Box to Shop.")
@commands.has_permissions(administrator=True)
async def addmysterybox(ctx, name: str, price: int):
    sid = f"A{get_next_id('shop_id')}"
    shop_col.insert_one({"id": sid, "name": name, "category": "Mystery Boxes", "price": price, "stock": -1})
    await ctx.send(f"Box {name} added.")

@bot.hybrid_command(name="addshinycoins", aliases=["asc"], description="Admin: Add Shiny Coins.")
@commands.has_permissions(administrator=True)
async def addshinycoins(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"shiny_coins": amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_SHINY} Added", f"Added {amount} SC to {member.mention}", 0x2ecc71))

# --- Standard Admin Commands (No Duplicates) ---
@bot.hybrid_command(name="payout", aliases=["po"], description="Admin: Pay user.")
@commands.has_permissions(administrator=True)
async def payout(ctx, user: discord.Member, amount: HumanAmount, *, reason: str):
    w = get_wallet(user.id)
    if not w or w.get("balance", 0) < amount: return await ctx.send("Insufficient funds.")
    wallets_col.update_one({"user_id": str(user.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(user.id, "Payout", f"${amount:,}")
    await send_log("withdraw", create_embed(f"{E_MONEY} Payout", f"**To:** {user.mention}\n**Amt:** ${amount:,}\n**Reason:** {reason}", 0xe74c3c))
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Paid", "Processed.", 0x2ecc71))

@bot.hybrid_command(name="logpayment", aliases=["lp"], description="Admin: Log manual.")
@commands.has_permissions(administrator=True)
async def logpayment(ctx, user: discord.Member, amount: HumanAmount, *, reason: str):
    await send_log("withdraw", create_embed(f"{E_ADMIN} Manual Log", f"**To:** {user.mention}\n**Amt:** ${amount:,}\n**Reason:** {reason}", 0x9b59b6))
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Logged", "Logged.", 0x2ecc71))

@bot.hybrid_command(name="tip", aliases=["t"], description="Admin Tip.")
@commands.has_permissions(administrator=True)
async def tip(ctx, member: discord.Member, amount: HumanAmount):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": amount}}, upsert=True)
    log_user_activity(member.id, "Tip", f"${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Tipped", f"Added ${amount:,}", 0x2ecc71))

@bot.hybrid_command(name="deduct_user", aliases=["du"], description="Admin Deduct.")
@commands.has_permissions(administrator=True)
async def deduct_user(ctx, member: discord.Member, amount: HumanAmount):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    log_user_activity(member.id, "Deduct", f"${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Deducted", f"Removed ${amount:,}", 0xe74c3c))

@bot.hybrid_command(name="playerhistory", aliases=["ph"], description="Admin: View full user history.")
@commands.has_permissions(administrator=True)
async def playerhistory(ctx, user: discord.Member):
    uid = str(user.id)
    w = get_wallet(uid)
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
    
    view = Paginator(ctx, data, f"{E_BOOK} History: {user.display_name}", 0x3498db)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="registerclub", description="Register club.")
@commands.has_permissions(administrator=True)
async def registerclub(ctx, name: str, base_price: int):
    cid = get_next_id("club_id")
    logo = ctx.message.attachments[0].url if ctx.message.attachments else ""
    clubs_col.insert_one({"id": cid, "name": name, "base_price": base_price, "value": base_price, "logo": logo, "total_wins": 0, "level_name": "5th Division"})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Registered", f"Club ID: {cid}", 0x2ecc71))

@bot.hybrid_command(name="deleteclub", description="Delete club.")
@commands.has_permissions(administrator=True)
async def deleteclub(ctx, name: str):
    clubs_col.delete_one({"name": name})
    await ctx.send(embed=create_embed(f"{E_DANGER} Deleted", "Club removed.", 0xff0000))

@bot.hybrid_command(name="startclubauction", description="Start club auction.")
@commands.has_permissions(administrator=True)
async def startclubauction(ctx, name: str):
    c = clubs_col.find_one({"name": name})
    if not c: return
    bids_col.delete_many({"item_type": "club", "item_id": c["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Auction Started", f"Bidding open for **{name}**", 0xe67e22))
    schedule_auction_timer("club", c["id"], ctx.channel.id)

@bot.hybrid_command(name="startduelistauction", description="Start duelist auction.")
@commands.has_permissions(administrator=True)
async def startduelistauction(ctx, did: int):
    bids_col.delete_many({"item_type": "duelist", "item_id": did})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Auction Started", f"Bidding open for Duelist ID {did}", 0x9b59b6))
    schedule_auction_timer("duelist", did, ctx.channel.id)

@bot.hybrid_command(name="registerbattle", description="Register battle.")
@commands.has_permissions(administrator=True)
async def registerbattle(ctx, c1: str, c2: str):
    bid = get_next_id("battle_id")
    battles_col.insert_one({"id": bid, "c1": c1, "c2": c2})
    await ctx.send(embed=create_embed(f"{E_FIRE} Battle Ready", f"Match ID: {bid}", 0xe74c3c))

@bot.hybrid_command(name="battleresult", description="Log result.")
@commands.has_permissions(administrator=True)
async def battleresult(ctx, bid: int, winner: str):
    wc = clubs_col.find_one({"name": winner})
    clubs_col.update_one({"_id": wc["_id"]}, {"$inc": {"value": WIN_VALUE_BONUS, "total_wins": 1}})
    update_club_level(wc['id'])
    
    banter = random.choice(BATTLE_BANTER).replace("{winner}", winner).replace("{loser}", "Loser")
    banter = banter.replace("{w_emoji}", resolve_emoji(random.choice(WINNER_REACTIONS)))
    banter = banter.replace("{l_emoji}", resolve_emoji(random.choice(LOSER_REACTIONS)))
    
    await ctx.send(embed=create_embed(f"{E_FIRE} Result", banter, 0xe74c3c))

@bot.hybrid_command(name="forcewinner", description="Force win.")
@commands.has_permissions(administrator=True)
async def forcewinner(ctx, type: str, id: int, winner: str, amt: int):
    bids_col.insert_one({"bidder": winner, "amount": amt, "item_type": type, "item_id": id})
    await finalize_auction(type, id, ctx.channel.id)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Force Win", "Auction ended manually.", 0xe67e22))

@bot.hybrid_command(name="freezeauction", description="Freeze.")
@commands.has_permissions(administrator=True)
async def freezeauction(ctx):
    global bidding_frozen
    bidding_frozen = True
    await ctx.send(embed=create_embed(f"{E_DANGER} Frozen", "Auctions paused.", 0xff0000))

@bot.hybrid_command(name="unfreezeauction", description="Unfreeze.")
@commands.has_permissions(administrator=True)
async def unfreezeauction(ctx):
    global bidding_frozen
    bidding_frozen = False
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Unfrozen", "Auctions resumed.", 0x2ecc71))

@bot.hybrid_command(name="setprefix", description="Set prefix.")
@commands.has_permissions(administrator=True)
async def setprefix(ctx, p: str):
    config_col.update_one({"key": "prefix"}, {"$set": {"value": p}}, upsert=True)
    await ctx.send(f"Prefix set to: {p}")

@bot.hybrid_command(name="admin_reset_all", description="Reset DB.")
@commands.has_permissions(administrator=True)
async def admin_reset_all(ctx):
    if ctx.author.id != BOT_OWNER_ID: return
    clubs_col.update_many({}, {"$set": {"owner_id": None, "value": 1000000, "total_wins": 0}})
    profiles_col.delete_many({})
    await ctx.send(embed=create_embed(f"{E_DANGER} Reset", "Season data wiped.", 0xff0000))

@bot.hybrid_command(name="checkclubmessages", description="Activity bonus.")
@commands.has_permissions(administrator=True)
async def checkclubmessages(ctx, name: str, count: int):
    if count > OWNER_MSG_COUNT_PER_BONUS:
        clubs_col.update_one({"name": name}, {"$inc": {"value": OWNER_MSG_VALUE_BONUS}})
        await ctx.send(embed=create_embed(f"{E_BOOST} Bonus", "Activity bonus applied.", 0x2ecc71))

@bot.hybrid_command(name="adjustgroupfunds", description="Edit funds.")
@commands.has_permissions(administrator=True)
async def adjustgroupfunds(ctx, name: str, amt: int):
    groups_col.update_one({"name": name}, {"$inc": {"funds": amt}})
    await ctx.send(embed=create_embed(f"{E_ADMIN} Adjusted", "Group funds updated.", 0x2ecc71))

@bot.hybrid_command(name="setclubmanager", description="Set manager.")
@commands.has_permissions(administrator=True)
async def setclubmanager(ctx, name: str, mem: discord.Member):
    clubs_col.update_one({"name": name}, {"$set": {"manager_id": str(mem.id)}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Manager Set", f"{member.mention} is now manager.", 0x2ecc71))

@bot.hybrid_command(name="auditlog", description="View logs.")
async def auditlog(ctx):
    logs = list(audit_col.find().sort("timestamp", -1).limit(10))
    txt = "\n".join([f"• {l['entry']}" for l in logs])
    await ctx.send(embed=create_embed(f"{E_BOOK} Audit Log", txt or "No logs.", 0x3498db))

@bot.hybrid_command(name="resetauction", description="Clear bids.")
async def resetauction(ctx):
    bids_col.delete_many({})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Cleared", "All active bids removed.", 0x2ecc71))

@bot.hybrid_command(name="transferclub", description="Transfer club.")
@commands.has_permissions(administrator=True)
async def transferclub(ctx, old: str, new: str):
    c = clubs_col.find_one({"owner_id": f"group:{old}"})
    if c: clubs_col.update_one({"id": c['id']}, {"$set": {"owner_id": f"group:{new}"}})
    await ctx.send(embed=create_embed(f"{E_ADMIN} Transferred", f"Club moved to {new}.", 0xe67e22))

# --- EVENTS ---

@bot.event
async def on_message(message):
    if message.author.bot:
        # PC Deposit (Poketwo Sync)
        if message.author.id == 716390085896962058 and message.embeds:
            emb = message.embeds[0]
            if "Balance" in emb.title:
                match = re.search(r"([\d,]+)\s*pc", emb.description, re.IGNORECASE)
                if match:
                    amount = int(match.group(1).replace(",", ""))
                    target_id = None
                    
                    # Strategy 1: Reply Reference
                    if message.reference:
                        try:
                            orig = await message.channel.fetch_message(message.reference.message_id)
                            target_id = str(orig.author.id)
                        except: pass
                    
                    # Strategy 2: Mention in text
                    if not target_id:
                        m = re.search(r"<@(\d+)>", emb.description)
                        if m: target_id = m.group(1)
                    
                    if target_id:
                        wallets_col.update_one({"user_id": target_id}, {"$set": {"pc": amount}}, upsert=True)
                        try: await message.add_reaction("✅") 
                        except: pass
        return

    # Daily Message Tracking
    if message.channel.id == CHAT_CHANNEL_ID:
        today = datetime.now().strftime("%Y-%m-%d")
        daily_stats_col.update_one(
            {"user_id": str(message.author.id), "date": today},
            {"$inc": {"count": 1}},
            upsert=True
        )
    
    await bot.process_commands(message)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    try:
        await bot.tree.sync()
        bot.add_view(GiveawayView()) 
        bot.add_view(ShopView())
        bot.loop.create_task(market_simulation_task())
    except Exception as e: print(e)

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)