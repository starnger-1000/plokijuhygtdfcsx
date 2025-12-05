# bot.py
# Full Club Auction Bot (Final Release - Blueprint 5.1 - Part 1 of 3)
# PART 1: Config, Database, Assets, Helpers, and UI Systems
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
#  1. CONFIGURATION & DATABASE CONNECTION
# ==============================================================================

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

# Owner Configuration
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID")) if os.getenv("BOT_OWNER_ID") else None

# Log Channels (Replace IDs with your actual channel IDs)
LOG_CHANNELS = {
    "withdraw": 1443955732281167873, 
    "battle": 1439844034905374720,
    "club": 1443955856222851142,
    "duelist": 1443955967086690395,
    "shop": 1443955732281167873,     # Admin Shop Sales
    "pending": 1443955732281167873,  # User Market Pending Deals
    "sold_out": 1443955732281167873, # User Market Completed Deals
    "giveaway": 1443955732281167873  # Giveaway Results
}

# Engagement Settings
CHAT_CHANNEL_ID = 123456789012345678 # Channel to track 100 messages
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

# Database Initialization
if not MONGO_URL:
    print("CRITICAL: MONGO_URL missing.")
    cluster = None; db = None
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
    giveaways_col = db.giveaways
    # NEW COLLECTIONS FOR SHOP/MARKET/DAILY
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
#  2. ASSETS: PREMIUM EMOJIS & BANTER
# ==============================================================================

# Visual Assets (No Default Emojis)
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

# Currency Specific Emojis (Updated to Blueprint 5.1)
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

# ---------- BOT SETUP & HELPERS ----------
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

# --- UTILS ---
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
    """Smart Wallet Fetcher (Handles String/Int IDs)"""
    if db is None: return None
    w = wallets_col.find_one({"user_id": str(user_id)})
    if w: return w
    w = wallets_col.find_one({"user_id": int(user_id)})
    if w:
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
        if not page_data: embed.description = "No data available."
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
            **{E_GIVEAWAY} Recent Major Updates (v5.1)**
            
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
            discord.SelectOption(label="Shiny Coins", emoji=E_SHINY, description="Buy Shiny Coins"),
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
        # Deduct Funds
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
            
            if not bidder.startswith('group:'): 
                profiles_col.update_one({"user_id": bidder}, {"$set": {"owned_club_id": int(item_id)}}, upsert=True)
            
            embed = create_embed(f"{E_GIVEAWAY} SOLD!", f"{E_SUCCESS} **New Owner:** {bidder}\n{E_MONEY} **Final Price:** ${amt:,}", 0xf1c40f, item.get('logo'))
            if ch: await ch.send(embed=embed)
            await send_log("club", embed)
        else:
            # Duelist Logic
            contracts_col.insert_one({"duelist_id": int(item_id), "club_owner": bidder, "amount": amt, "timestamp": datetime.now()})
            cid = None
            if bidder.startswith('group:'): 
                c = clubs_col.find_one({"owner_id": bidder})
                if c: cid = c['id']
            else: 
                c = clubs_col.find_one({"owner_id": bidder})
                if c: cid = c['id']
            
            duelists_col.update_one({"id": int(item_id)}, {"$set": {"owned_by": bidder, "club_id": cid}})
            # Pay the player the signing fee
            wallets_col.update_one({"user_id": item["discord_user_id"]}, {"$inc": {"balance": amt}}, upsert=True)
            
            embed = create_embed(f"{E_GIVEAWAY} SIGNED!", f"{E_SUCCESS} **Signed To:** {bidder}\n{E_MONEY} **Transfer Fee:** ${amt:,} (Paid to Player)", 0x9b59b6, item.get('avatar_url'))
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
#  CORE COMMANDS
# ==============================================================================

@bot.hybrid_command(name="botinfo", aliases=["info", "help", "helpme"], description="Show bot information and help.")
async def botinfo(ctx):
    desc = f"""
    Welcome to the **Football x Poketwo** Kingdom! 🏰
    This bot manages the entire server economy, transfer market, auctions, and duelist careers.
    
    **Features:**
    • 💰 **Economy:** Earn, save, and burn cash.
    • 🤝 **Groups:** Pool funds with friends to buy massive clubs.
    • 🔨 **Auctions:** Live bidding wars for Clubs & Players.
    • ⚽ **Duelists:** Sign up as a player and get signed by a club for a salary.
    • 🏟️ **Matches:** Simulate matches and rise through the divisions.
    • {E_STARS} **Shop:** Buy items, shiny coins, and rare Pokemon.
    
    Select a category from the dropdown below to explore commands!
    """
    embed = create_embed(f"{E_CROWN} Club Auction Bot", desc, 0x3498db, bot.user.avatar.url if bot.user.avatar else None)
    embed.add_field(name="Stats", value=f"ping: {round(bot.latency*1000)}ms\nServers: {len(bot.guilds)}")
    view = BotInfoView()
    await ctx.send(embed=embed, view=view)

# ==============================================================================
#  ECONOMY & USER COMMANDS
# ==============================================================================

@bot.hybrid_command(name="profile", aliases=["p"], description="View your profile.")
async def profile(ctx, member: discord.Member = None):
    member = member or ctx.author
    uid = str(member.id)
    w = get_wallet(uid)
    bal_cash = w.get("balance", 0) if w else 0
    bal_sc = w.get("shiny_coins", 0) if w else 0
    bal_pc = w.get("pc", 0) if w else 0
    
    thumbnail_url = member.avatar.url if member.avatar else None
    group_mem = group_members_col.find_one({"user_id": uid})
    if group_mem:
        g_info = groups_col.find_one({"name": group_mem['group_name']})
        if g_info and g_info.get('logo'): thumbnail_url = g_info['logo']

    embed = create_embed(f"{E_CROWN} Profile", f"**User:** {member.mention}", 0x3498db, thumbnail=thumbnail_url)
    
    # 3-Currency Display
    embed.add_field(name=f"{E_MONEY} Cash", value=f"${bal_cash:,}", inline=True)
    embed.add_field(name=f"{E_SHINY} Shiny Coins", value=f"{bal_sc:,}", inline=True)
    embed.add_field(name=f"{E_PC} Pokécoins", value=f"{bal_pc:,}", inline=True)
    
    groups = list(group_members_col.find({"user_id": uid}))
    g_list = [f"{g['group_name'].title()} ({g['share_percentage']}%)" for g in groups]
    embed.add_field(name=f"{E_PREMIUM} Groups", value=", ".join(g_list) if g_list else "None", inline=False)
    
    prof = profiles_col.find_one({"user_id": uid})
    if prof and prof.get("owned_club_id"):
        c = clubs_col.find_one({"id": prof["owned_club_id"]})
        if c: embed.add_field(name=f"{E_CROWN} Owned Club", value=f"{c['name']} (100%)", inline=False)
    
    past_clubs = list(past_entities_col.find({"user_id": uid, "type": "ex_owner"}))
    pc_list = [p['name'] for p in past_clubs]
    if pc_list: embed.add_field(name=f"{E_RED_ARROW} Ex-Owner", value=", ".join(pc_list), inline=False)

    duelist = duelists_col.find_one({"discord_user_id": uid})
    if duelist:
        cname = "Free Agent"
        if duelist.get("club_id"):
            c = clubs_col.find_one({"id": duelist["club_id"]})
            if c: cname = c["name"]
        embed.add_field(name=f"{E_ITEMBOX} Duelist Status", value=f"Club: {cname}\nSalary: ${duelist['expected_salary']:,}", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="wallet", aliases=["bal"], description="Check balance.")
async def wallet(ctx):
    w = get_wallet(ctx.author.id)
    bal = w.get("balance", 0) if w else 0
    sc = w.get("shiny_coins", 0) if w else 0
    pc = w.get("pc", 0) if w else 0
    desc = f"{E_MONEY} **Cash:** ${bal:,}\n{E_SHINY} **Shiny Coins:** {sc:,}\n{E_PC} **Pokécoins:** {pc:,}"
    await ctx.send(embed=create_embed(f"{E_MONEY} Wallet Balance", desc, 0x2ecc71))

@bot.hybrid_command(name="withdrawwallet", aliases=["ww"], description="Burn money.")
async def withdrawwallet(ctx, amount: HumanAmount):
    if amount <= 0: return
    w = get_wallet(ctx.author.id)
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Burned ${amount:,} from wallet.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdrawn", f"Removed **${amount:,}** from wallet.", 0x2ecc71))

@bot.hybrid_command(name="daily", description="Claim daily reward (Requires 100 msgs).")
async def daily(ctx):
    today = datetime.now().strftime("%Y-%m-%d")
    stats = daily_stats_col.find_one({"user_id": str(ctx.author.id), "date": today})
    count = stats["count"] if stats else 0
    
    if count < DAILY_MSG_REQ:
        needed = DAILY_MSG_REQ - count
        return await ctx.send(embed=create_embed(f"{E_DANGER} Locked", f"You need **{needed}** more messages in <#{CHAT_CHANNEL_ID}> today!", 0xff0000))
    
    last = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if last and last.get("last_daily") == today:
        return await ctx.send(embed=create_embed(f"{E_ALERT} Cooldown", "You already claimed today!", 0xff0000))
        
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": DAILY_REWARD_AMOUNT}, "$set": {"last_daily": today}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Daily Claimed", f"You received **${DAILY_REWARD_AMOUNT:,}**!", 0x2ecc71))

@bot.hybrid_command(name="redeem", description="Redeem a code.")
async def redeem(ctx, code: str):
    c = redeem_codes_col.find_one({"code": code})
    if not c: return await ctx.send(embed=create_embed("Error", "Invalid code.", 0xff0000))
    if c["uses"] >= c["max_uses"]: return await ctx.send(embed=create_embed("Error", "Code expired.", 0xff0000))
    if ctx.author.id in c["claimed_by"]: return await ctx.send(embed=create_embed("Error", "Already redeemed.", 0xff0000))
    
    if c["type"] == "cash": wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": c["amount"]}}, upsert=True)
    elif c["type"] == "sc": wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"shiny_coins": c["amount"]}}, upsert=True)
    elif c["type"] == "pc": wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"pc": c["amount"]}}, upsert=True)
    
    redeem_codes_col.update_one({"_id": c["_id"]}, {"$inc": {"uses": 1}, "$push": {"claimed_by": ctx.author.id}})
    await ctx.send(embed=create_embed(f"{E_GIVEAWAY} Redeemed!", f"You got **{c['amount']:,} {c['type'].upper()}**!", 0x2ecc71))

# ==============================================================================
#  GROUP COMMANDS
# ==============================================================================

@bot.hybrid_command(name="creategroup", description="Create group.")
async def creategroup(ctx, name: str, share: int):
    gname = name.lower()
    if groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group exists.", 0xff0000))
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    groups_col.insert_one({"name": gname, "funds": 0, "owner_id": str(ctx.author.id), "logo": logo_url})
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Created group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Created", f"Group **{name}** created with **{share}%** share.\nLogo Set: {'Yes' if logo_url else 'No'}", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="joingroup", description="Join group.")
async def joingroup(ctx, name: str, share: int):
    gname = name.lower()
    if not groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already a member.", 0xff0000))
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Joined group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Joined", f"Joined **{name}** with **{share}%**.", 0x2ecc71))

@bot.hybrid_command(name="deposit", aliases=["dep"], description="Deposit to group.")
async def deposit(ctx, group_name: str, amount: HumanAmount):
    if amount <= 0: return
    w = get_wallet(ctx.author.id)
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": amount}})
    log_user_activity(ctx.author.id, "Dep", f"${amount:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deposited", f"${amount:,} to {group_name}", 0x2ecc71))

@bot.hybrid_command(name="withdraw", aliases=["wd"], description="Withdraw group.")
async def withdraw(ctx, group_name: str, amount: HumanAmount):
    g = groups_col.find_one({"name": group_name.lower()})
    if not g or g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient group funds.", 0xff0000))
    if not group_members_col.find_one({"group_name": group_name.lower(), "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": -amount}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": amount}})
    log_user_activity(ctx.author.id, "WD", f"${amount:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdrew", f"${amount:,} from {group_name}", 0x2ecc71))

@bot.hybrid_command(name="leavegroup", description="Leave group.")
async def leavegroup(ctx, name: str):
    gname = name.lower()
    mem = group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)})
    if not mem: return await ctx.send("Not member.")
    g = groups_col.find_one({"name": gname})
    penalty = int(g["funds"] * (LEAVE_PENALTY_PERCENT / 100))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -penalty}})
    group_members_col.delete_one({"_id": mem["_id"]})
    log_past_entity(ctx.author.id, "ex_member", gname)
    await ctx.send(embed=create_embed(f"{E_DANGER} Left Group", f"Left **{name}**. Penalty: **${penalty:,}**.", 0xff0000))

@bot.hybrid_command(name="groupinfo", aliases=["gi"], description="Group info.")
async def groupinfo(ctx, *, group_name: str):
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Group not found.", 0xff0000))
    members = list(group_members_col.find({"group_name": gname}))
    embed = discord.Embed(title=f"{E_PREMIUM} Group: {g['name'].title()}", color=0x9b59b6)
    if g.get('logo'): embed.set_thumbnail(url=g['logo'])
    embed.add_field(name=f"{E_MONEY} Bank", value=f"${g['funds']:,}", inline=True)
    
    mlist = []
    for m in members[:15]:
        try: u = await bot.fetch_user(int(m['user_id'])); name = u.name
        except: name = "Unknown"
        mlist.append(f"{E_ARROW} {name}: {m['share_percentage']}%")
    embed.add_field(name=f"{E_ARROW} Members", value="\n".join(mlist) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="grouplist", description="List groups.")
async def grouplist(ctx):
    groups = list(groups_col.find())
    data = []
    for g in groups: data.append((g['name'].title(), f"{E_MONEY} ${g['funds']:,}"))
    view = Paginator(ctx, data, f"{E_PREMIUM} Group List", 0x9b59b6)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="depositwallet", description="Restricted command.")
async def depositwallet(ctx, amount: int = None):
    await ctx.send(embed=create_embed(f"{E_DANGER} Restricted", "Use `.deposit <Group> <Amt>` to fund group.", 0xff0000))

# ==============================================================================
#  MARKET & AUCTION COMMANDS
# ==============================================================================

@bot.hybrid_command(name="placebid", aliases=["pb"], description="Place a bid.")
async def placebid(ctx, amount: HumanAmount, item_type: str, item_id: int, club_name: str = None):
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
        c = clubs_col.find_one({"id": item_id})
        is_active = (item_type, str(item_id)) in active_timers
        if c.get("owner_id") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", "Owned.", 0xff0000))
        prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
        if prof and prof.get("owned_club_id"): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You already own a club (100%). Sell it first.", 0xff0000))

    w = get_wallet(ctx.author.id)
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    
    req = min_required_bid(get_current_bid(item_type, item_id))
    if amount < req: return await ctx.send(embed=create_embed("Bid Error", f"Min bid is ${req:,}", 0xff0000))
    
    bids_col.insert_one({"bidder": str(ctx.author.id), "amount": amount, "item_type": item_type, "item_id": int(item_id), "timestamp": datetime.now()})
    log_user_activity(ctx.author.id, "Bid", f"{amount}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Bid Placed", f"Bid of **${amount:,}** accepted.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="groupbid", aliases=["gb"], description="Group bid.")
async def groupbid(ctx, group_name: str, amount: HumanAmount, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", "Auctions frozen.", 0xff0000))
    g = groups_col.find_one({"name": group_name.lower()})
    if not g: return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if not group_members_col.find_one({"group_name": group_name.lower(), "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         is_active = (item_type, str(item_id)) in active_timers
         if c.get("owner_id") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This club is **SOLD OUT**.", 0xff0000))
         if clubs_col.find_one({"owner_id": f"group:{group_name.lower()}"}): return await ctx.send(embed=create_embed("Error", "Group already owns a club.", 0xff0000))

    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    bids_col.insert_one({"bidder": f"group:{group_name.lower()}", "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    log_user_activity(ctx.author.id, "Bid", f"Group bid ${amount:,} on {item_type} {item_id}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Bid", f"Group **{group_name}** bid **${amount:,}**.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="sellclub", aliases=["sc"], description="Sell club.")
async def sellclub(ctx, club_name: str, buyer: discord.Member = None):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    if str(ctx.author.id) != c.get("owner_id"): return await ctx.send(embed=create_embed("Error", "You don't own this.", 0xff0000))
    val = c["value"]
    
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Sale", f"Sell **{c['name']}** to {target.mention if buyer else 'Market'} for **${val:,}**?\nType `yes` or `no`.", 0xe67e22))
    def check(m): return m.author == (buyer if buyer else ctx.author) and m.content.lower() in ['yes', 'no']
    try: msg = await bot.wait_for('message', check=check, timeout=30.0)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    if msg.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled.", 0x95a5a6))

    old_owner = c.get("owner_id")
    if old_owner:
        profiles_col.update_one({"user_id": old_owner}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        log_past_entity(old_owner, "ex_owner", c['name'])

    if buyer:
        bw = get_wallet(buyer.id)
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": str(buyer.id), "ex_owner_id": old_owner}})
        profiles_col.update_one({"user_id": str(buyer.id)}, {"$set": {"owned_club_id": c["id"], "owned_club_share": 100}}, upsert=True)
        profiles_col.update_one({"user_id": str(ctx.author.id)}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        log_user_activity(buyer.id, "Transaction", f"Bought {c['name']} for ${val:,}")
    else:
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": None, "ex_owner_id": old_owner}})
        profiles_col.update_one({"user_id": str(ctx.author.id)}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
    embed_log = create_embed(f"{E_ADMIN} Club Sold", f"**Club:** {c['name']}\n**Seller:** {ctx.author.mention}\n**Buyer:** {target.mention if buyer else 'Market'}\n**Price:** ${val:,}", 0xe67e22)
    await send_log("club", embed_log)
    log_user_activity(ctx.author.id, "Sale", f"Sold club {c['name']} for ${val:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", f"Club sold for **${val:,}**.", 0x2ecc71))

@bot.hybrid_command(name="sellshares", aliases=["ss"], description="Sell shares.")
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
    def check(m): return m.author == buyer and m.content.lower() in ['yes', 'no']
    try: msg = await bot.wait_for('message', check=check, timeout=30)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    
    bw = get_wallet(buyer.id)
    if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
    
    wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
    group_members_col.update_one({"_id": seller["_id"]}, {"$inc": {"share_percentage": -percentage}})
    group_members_col.update_one({"group_name": gname, "user_id": str(buyer.id)}, {"$inc": {"share_percentage": percentage}}, upsert=True)
    log_user_activity(ctx.author.id, "Sale", f"Sold {percentage}% shares of {gname}.")
    log_user_activity(buyer.id, "Transaction", f"Bought {percentage}% shares of {gname}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", "Shares transferred.", 0x2ecc71))

@bot.hybrid_command(name="listclubs", aliases=["lc"], description="List clubs.")
async def listclubs(ctx):
    clubs = list(clubs_col.find().sort("value", -1))
    data = []
    for c in clubs:
        data.append((f"{E_STAR} {c['name']} (ID: {c['id']})", f"{E_MONEY} ${c['value']:,}"))
    view = Paginator(ctx, data, f"{E_CROWN} Registered Clubs", 0x3498db)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="clubinfo", aliases=["ci"], description="Club info.")
async def clubinfo(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    
    owner_display = c.get('owner_id') or "Unowned"
    shareholder_text = ""
    if owner_display.startswith('group:'): 
        gname = owner_display.replace('group:', '').title()
        owner_display = f"Group: {gname}"
        raw_gname = c.get('owner_id').replace('group:', '').lower()
        members = list(group_members_col.find({"group_name": raw_gname}))
        shares = []
        for m in members:
             try: u = await bot.fetch_user(int(m['user_id'])); name = u.name
             except: name = "Unknown"
             if m['share_percentage'] > 0: shares.append(f"{name}: {m['share_percentage']}%")
        if shares: shareholder_text = "\n**Shareholders:**\n" + "\n".join(shares)
    else:
        try:
            if owner_display != "Unowned":
                owner_user = await bot.fetch_user(int(owner_display))
                owner_display = f"User: {owner_user.display_name}"
        except: pass
    
    ex_owner_display = "None"
    if c.get("ex_owner_id"):
        try: 
             ex_user = await bot.fetch_user(int(c["ex_owner_id"]))
             ex_owner_display = ex_user.display_name
        except: pass
    
    manager_name = "None"
    if c['manager_id']:
        try: u = await bot.fetch_user(int(c['manager_id'])); manager_name = u.name
        except: pass
    
    duelists = list(duelists_col.find({"club_id": c['id']}))
    d_list = "\n".join([f"{E_ARROW} {d['username']}" for d in duelists]) or "None"
    
    embed = discord.Embed(title=f"{E_CROWN} {c['name']}", description=f"{E_BOOST} **{c.get('level_name')}**{shareholder_text}", color=0x3498db)
    if c.get("logo") and isinstance(c["logo"], str) and (c["logo"].startswith("http://") or c["logo"].startswith("https://")):
        embed.set_thumbnail(url=c["logo"])
    embed.add_field(name=f"{E_STAR} Owner", value=owner_display, inline=True)
    embed.add_field(name=f"{E_RED_ARROW} Ex-Owner", value=ex_owner_display, inline=True)
    embed.add_field(name=f"{E_MONEY} Value", value=f"${c['value']:,}", inline=True)
    embed.add_field(name=f"{E_ADMIN} Manager", value=manager_name, inline=True)
    embed.add_field(name=f"{E_ITEMBOX} Duelists", value=d_list, inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="marketbuy", aliases=["mb"], description="Buy unowned.")
async def marketbuy(ctx, club_id: int, group_name: str = None):
    c = clubs_col.find_one({"id": club_id})
    if not c or c.get("owner_id"): return await ctx.send(embed=create_embed("Error", "Club invalid.", 0xff0000))
    price = c["value"]
    buyer_id = str(ctx.author.id)
    is_group = False
    
    if group_name:
        g = groups_col.find_one({"name": group_name.lower()})
        if not g or g["funds"] < price: return await ctx.send(embed=create_embed("Error", "Funds low.", 0xff0000))
        buyer_id = f"group:{group_name.lower()}"
        is_group = True
    else:
        w = get_wallet(ctx.author.id)
        if not w or w.get("balance", 0) < price: return await ctx.send(embed=create_embed("Error", "Funds low.", 0xff0000))
        
    view = MarketConfirmView(ctx, club_id, price, buyer_id, is_group)
    await ctx.send(embed=create_embed(f"{E_AUCTION} Confirm", f"Buy for ${price:,}?", 0xe67e22), view=view)

@bot.hybrid_command(name="marketlist", aliases=["ml"], description="Market.")
async def marketlist(ctx):
    clubs = list(clubs_col.find({"owner_id": None}))
    data = []
    for c in clubs: data.append((f"{E_STAR} {c['name']}", f"{E_MONEY} ${c['value']:,}"))
    view = Paginator(ctx, data, f"{E_AUCTION} Market Listings", 0xe67e22)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="leaderboard", aliases=["lb"], description="Leaderboard.")
async def leaderboard(ctx):
    clubs = list(clubs_col.find().sort([("total_wins", -1)]))
    data = []
    for i, c in enumerate(clubs):
        data.append((f"{E_CROWN} #{i+1} {c['name']}", f"{E_FIRE} {c.get('total_wins',0)} Wins"))
    view = Paginator(ctx, data, f"{E_CROWN} Club Leaderboard", 0xf1c40f)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="clublevel", description="Club Level.")
async def clublevel(ctx, *, name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})
    if not c: return
    await ctx.send(embed=create_embed(f"{E_BOOST} Club Level", f"**{c.get('level_name')}**\n{E_FIRE} Wins: {c.get('total_wins',0)}", 0xf1c40f))

@bot.hybrid_command(name="marketpanel", description="Market Panel.")
async def marketpanel(ctx, *, name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})
    if not c: return
    await ctx.send(embed=create_embed(f"{E_STARS} Market Panel", f"**Value:** ${c['value']:,}", 0x3498db, c.get('logo')))
# ==============================================================================
#  DUELIST COMMANDS
# ==============================================================================

@bot.hybrid_command(name="registerduelist", aliases=["rd"], description="Register as a duelist.")
async def registerduelist(ctx, username: str, base_price: HumanAmount, salary: HumanAmount):
    if duelists_col.find_one({"discord_user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already registered.", 0xff0000))
    did = get_next_id("duelist_id")
    avatar = ctx.author.avatar.url if ctx.author.avatar else ""
    duelists_col.insert_one({"id": did, "discord_user_id": str(ctx.author.id), "username": username, "base_price": base_price, "expected_salary": salary, "avatar_url": avatar, "owned_by": None, "club_id": None})
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
        # Logic to notify owner could be added here
        pass 
        
    duelists_col.delete_one({"_id": d["_id"]})
    await ctx.send(embed=create_embed(f"{E_DANGER} Retired", "Duelist profile deleted.", 0xff0000))

@bot.hybrid_command(name="adjustsalary", aliases=["as"], description="Owner: Bonus or Fine.")
async def adjustsalary(ctx, duelist_id: int, amount: HumanAmount):
    d = duelists_col.find_one({"id": duelist_id})
    if not d or not d.get("club_id"): return await ctx.send("Invalid duelist.")
    
    owner_str, owner_ids = get_club_owner_info(d["club_id"])
    if str(ctx.author.id) not in owner_ids: return await ctx.send("Not owner.")

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
#  SHOP, INVENTORY & MYSTERY BOX
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
    # 1. Check User Market (ID starts with U) - Uses PC
    if item_id.startswith("U"):
        listing = market_col.find_one({"id": item_id})
        if not listing: return await ctx.send("Listing not found.")
        if listing.get("status") == "pending": return await ctx.send("Item is pending approval.")
        
        price = listing['price']
        tax = int(price * 0.025) # 2.5% Buyer Tax
        total_cost = price + tax
        
        w = get_wallet(ctx.author.id)
        if not w or w.get("pc", 0) < total_cost: return await ctx.send(embed=create_embed("Error", f"Insufficient PC. Need {total_cost:,} PC.", 0xff0000))
        
        # Deduct & Hold
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"pc": -total_cost}})
        market_col.update_one({"id": item_id}, {"$set": {"status": "pending", "buyer_id": str(ctx.author.id)}})
        
        # Notify Admin
        chn = bot.get_channel(LOG_CHANNELS["pending"]) 
        if chn:
            # Simple Approval View
            view = View(timeout=None)
            
            async def approve_cb(interaction):
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
                # Refund Tax to Buyer, Full Price to Seller
                wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"pc": tax}}) # Refund tax
                wallets_col.update_one({"user_id": listing['seller_id']}, {"$inc": {"pc": price}}) # Full price
                
                # Transfer
                inventory_col.update_one({"user_id": str(ctx.author.id), "item_id": listing['p_id']}, {"$inc": {"quantity": 1}, "$set": {"name": listing['name'], "type": "Pokemon"}}, upsert=True)
                market_col.delete_one({"id": item_id})
                
                await interaction.message.delete()
                await interaction.channel.send(embed=create_embed(f"{E_STARS} Premium Approved", "Tax waived.", 0x2ecc71))

            async def decline_cb(interaction):
                # Refund Buyer
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

    # 2. Admin Shop (ID starts with A or numeric) - Uses SC
    else:
        # Handle ID lookup (Int or String "A1")
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
        
        # Scraping
        name = emb.title or "Unknown Pokemon"
        img = emb.thumbnail.url if emb.thumbnail else ""
        desc = emb.description or ""
        
        # Auto-Tagging
        tags = []
        if "✨" in name: tags.append("Shiny")
        if "Legendary" in desc: tags.append("Rare")
        if "Fire" in desc: tags.append("Fire")
        # ... (Add more type checks as needed)
        
        uid = f"U{get_next_id('market_id')}"
        
        # Create Listing
        market_col.insert_one({
            "id": uid, "p_id": f"P{get_next_id('poke_data_id')}", "name": name, 
            "price": price, "seller_id": str(ctx.author.id), "tags": tags, 
            "status": "active", "buyer_id": None
        })
        
        # Save Stats
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
                    user_id_match = re.search(r"<@(\d+)>", emb.description)
                    if user_id_match:
                        uid = user_id_match.group(1)
                        wallets_col.update_one({"user_id": uid}, {"$set": {"pc": amount}}, upsert=True)
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