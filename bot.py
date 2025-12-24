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

LEVEL_UP_CONFIG = [
    (12, "5th Division", 50000), (27, "4th Division", 100000), (45, "3rd Division", 150000),
    (66, "2nd Division", 200000), (90, "1st Division", 300000), (117, "17th Position", 320000),
    (147, "15th Position", 360000), (180, "12th Position", 400000), (216, "10th Position", 450000),
    (255, "8th Position", 500000), (297, "6th Position", 550000), (342, "Conference League", 600000),
    (390, "5th Position", 650000), (441, "Europa League", 700000), (495, "4th Position", 750000),
    (552, "3rd Position", 800000), (612, "Champions League", 900000), (675, "2nd Position", 950000),
    (741, "1st Position and League Winner", 1000000), (810, "UCL Winner", 1500000), (882, "Treble Winner", 2000000),
]

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
    inventory_col = db.inventory
    coupons_col = db.coupons
    redeem_codes_col = db.redeem_codes
    message_counts_col = db.message_counts
    pending_shop_approvals = db.pending_shop_approvals # New collection for Shop Approvals

def get_next_id(sequence_name):
    if db is None: return 0
    ret = counters_col.find_one_and_update(
        {"_id": sequence_name}, {"$inc": {"seq": 1}},
        upsert=True, return_document=ReturnDocument.AFTER
    )
    return ret['seq']

# ---------- BOT SETUP ----------
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
    while True:
        await asyncio.sleep(3600)
        if db is not None:
            for c in clubs_col.find():
                base = c.get("value", c["base_price"])
                change = random.uniform(-0.03, 0.03)
                new_value = int(max(100, base * (1 + change)))
                clubs_col.update_one({"_id": c["_id"]}, {"$set": {"value": new_value}})

@bot.event
async def on_command_completion(ctx):
    log_user_activity(ctx.author.id, "Command", f"Used {E_CHAT} `.{ctx.command.name}`")

@bot.event
async def on_message(message):
    if message.author.bot: return
    # Check for Specific Chat Channel
    if db is not None and message.channel.id == LOG_CHANNELS["chat_channel"]:
        today_str = datetime.now().strftime("%Y-%m-%d")
        message_counts_col.update_one(
            {"user_id": str(message.author.id), "date": today_str},
            {"$inc": {"count": 1}},
            upsert=True
        )
    await bot.process_commands(message)

# ===========================
#   GROUP 1: ECONOMY & PROFILE
# ===========================

@bot.hybrid_command(name="playerhistory", aliases=["ph"], description="Admin: View full user history.")
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

@bot.hybrid_command(name="profile", aliases=["pr"], description="View profile stats and currencies.")
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
    await ctx.send(embed=embed)

@bot.hybrid_command(name="wallet", aliases=["wl"], description="Check your balance.")
async def wallet(ctx):
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    cash = w.get("balance", 0) if w else 0
    pc = w.get("pc", 0) if w else 0
    shiny = w.get("shiny_coins", 0) if w else 0
    embed = create_embed(f"{E_MONEY} Wallet Balance", f"**User:** {ctx.author.mention}\n\n{E_MONEY} **Cash:** ${cash:,}\n{E_PC} **PC:** {pc:,}\n{E_SHINY} **Shiny Coins:** {shiny:,}", 0x2ecc71, thumbnail=ctx.author.avatar.url if ctx.author.avatar else None)
    await ctx.send(embed=embed)

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
    await ctx.send(embed=create_embed(f"{E_GIVEAWAY} Daily Claimed", f"You received:\n+$10,000 {E_MONEY}\n+5 {E_SHINY}", 0x2ecc71))

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

@bot.hybrid_command(name="creategroup", aliases=["cg"], description="Create a new investment group.")
async def creategroup(ctx, name: str, share: int):
    gname = name.lower()
    if groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group exists.", 0xff0000))
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    groups_col.insert_one({"name": gname, "funds": 0, "owner_id": str(ctx.author.id), "logo": logo_url})
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Created group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Created", f"Group **{name}** created with **{share}%** share.", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="joingroup", aliases=["jg"], description="Join an existing group.")
async def joingroup(ctx, name: str, share: int):
    gname = name.lower()
    if not groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already a member.", 0xff0000))
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Joined group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Joined", f"Joined **{name}** with **{share}%**.", 0x2ecc71))

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
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", "Shares transferred.", 0x2ecc71))

@bot.hybrid_command(name="marketlist", aliases=["ml"], description="View unsold clubs.")
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

@bot.hybrid_command(name="listclubs", aliases=["lc"], description="List all registered clubs.")
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

@bot.hybrid_command(name="leaderboard", aliases=["lb"], description="View top clubs.")
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
    if duelists_col.find_one({"discord_user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already registered.", 0xff0000))
    did = get_next_id("duelist_id")
    avatar = ctx.author.avatar.url if ctx.author.avatar else ""
    duelists_col.insert_one({"id": did, "discord_user_id": str(ctx.author.id), "username": username, "base_price": base_price, "expected_salary": salary, "avatar_url": avatar, "owned_by": None, "club_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Registered", f"Duelist **{username}** (ID: {did})", 0x9b59b6))

@bot.hybrid_command(name="retireduelist", aliases=["ret"], description="Retire a duelist.")
async def retireduelist(ctx, member: discord.Member = None):
    target_id = str(member.id) if member else str(ctx.author.id)
    d = duelists_col.find_one({"discord_user_id": target_id})
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
        if d.get("owned_by"): return await ctx.send(embed=create_embed("Error", "You are signed. Ask owner.", 0xff0000))
    duelists_col.delete_one({"_id": d["_id"]})
    embed_log = create_embed(f"{E_DANGER} Duelist Retired", f"**Player:** {d['username']}\n**ID:** {d['id']}", 0xff0000)
    await send_log("duelist", embed_log)
    log_user_activity(target_id, "Duelist", "Retired")
    await ctx.send(embed=create_embed(f"{E_DANGER} Retired", f"Duelist **{d['username']}** retired.", 0xff0000))

@bot.hybrid_command(name="listduelists", aliases=["ld"], description="List duelists.")
async def listduelists(ctx):
    ds = list(duelists_col.find())
    data = []
    for d in ds:
        cname = "Free Agent"
        if d.get("club_id"):
            c = clubs_col.find_one({"id": d["club_id"]})
            if c: cname = c["name"]
        data.append((f"{d['username']}", f"{E_ITEMBOX} ID: {d['id']}\n{E_MONEY} ${d['expected_salary']:,}\n{E_STAR} {cname}"))
    view = Paginator(ctx, data, f"{E_BOOK} Duelist Registry", 0x9b59b6, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="adjustsalary", aliases=["as"], description="Owner: Bonus/Fine.")
async def adjustsalary(ctx, duelist_id: int, amount: HumanInt):
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d or not d.get("club_id"): return await ctx.send(embed=create_embed("Error", "Duelist not found/signed.", 0xff0000))
    c = clubs_col.find_one({"id": d["club_id"]})
    owner_str, owner_ids = get_club_owner_info(c["id"])
    if str(ctx.author.id) not in owner_ids: return await ctx.send(embed=create_embed("Error", "Not owner.", 0xff0000))
    if amount > 0:
        w = wallets_col.find_one({"user_id": str(ctx.author.id)})
        if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
        wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
        log_user_activity(ctx.author.id, "Transaction", f"Paid bonus ${amount:,} to {d['username']}")
        await ctx.send(embed=create_embed(f"{E_MONEY} Bonus", f"Paid **${amount:,}** to {d['username']}.", 0x2ecc71))
    else:
        abs_amt = abs(amount)
        wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": -abs_amt}}, upsert=True)
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": abs_amt}}, upsert=True)
        log_user_activity(ctx.author.id, "Transaction", f"Fined {d['username']} ${abs_amt:,}")
        await ctx.send(embed=create_embed(f"{E_DANGER} Fine", f"Deducted **${abs_amt:,}** from {d['username']}.", 0xff0000))

@bot.hybrid_command(name="deductsalary", aliases=["ds"], description="Owner: Deduct salary.")
async def deductsalary(ctx, duelist_id: int, confirm: str):
    if confirm.lower() != "yes": return
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d: return await ctx.send(embed=create_embed("Error", "Duelist not found.", 0xff0000))
    if not d.get('club_id'): return await ctx.send(embed=create_embed("Error", "Duelist not in a club.", 0xff0000))
    owner_str, owner_ids = get_club_owner_info(d['club_id'])
    if str(ctx.author.id) not in owner_ids and not ctx.author.guild_permissions.administrator: return await ctx.send(embed=create_embed("Error", "Not authorized.", 0xff0000))
    penalty = int(d["expected_salary"] * (DUELIST_MISS_PENALTY_PERCENT / 100))
    wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": -penalty}}, upsert=True)
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": penalty}}, upsert=True)
    log_user_activity(d["discord_user_id"], "Penalty", f"Fined ${penalty:,} for missed match.")
    await ctx.send(embed=create_embed(f"{E_ALERT} Penalty", f"Fined **${penalty:,}** from **{d['username']}**'s wallet.", 0xff0000))


# ===========================
#   GROUP 4: ADMIN
# ===========================

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

@bot.hybrid_command(name="setprefix", aliases=["sp"], description="Admin: Change prefix.")
@commands.has_permissions(administrator=True)
async def setprefix(ctx, new_prefix: str):
    config_col.update_one({"key": "prefix"}, {"$set": {"value": new_prefix}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Prefix Updated", f"New prefix: **`{new_prefix}`**", 0x2ecc71))

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

@bot.hybrid_command(name="freezeauction", aliases=["fa"], description="Owner: Freeze auctions.")
@commands.has_permissions(administrator=True)
async def freezeauction(ctx):
    global bidding_frozen
    bidding_frozen = True
    await ctx.send(embed=create_embed(f"{E_DANGER} Frozen", "Auctions frozen.", 0xff0000))

@bot.hybrid_command(name="unfreezeauction", aliases=["ufa"], description="Owner: Resume auctions.")
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

            # Transfer Ownership
            clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": buyer_id}})
            
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

@bot.hybrid_command(name="servertradehistory", aliases=["sth"], description="View global server trade logs.")
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

# ===========================
#   GROUP 6: NEW SHOP & INVENTORY
# ===========================

@bot.hybrid_command(name="addshopitem", description="Admin: Add Item to Shop.")
@commands.has_permissions(administrator=True)
async def addshopitem(ctx, name: str, price: int, image: discord.Attachment = None):
    item_id = f"A{get_next_id('shop_item_id')}" 
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

@bot.hybrid_command(name="addshinycoins", description="Admin: Grant Shiny Coins.")
@commands.has_permissions(administrator=True)
async def addshinycoins(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"shiny_coins": amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Grant", f"Added **{amount:,}** {E_SHINY} to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="addpc", description="Admin: Grant PC.")
@commands.has_permissions(administrator=True)
async def addpc(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"pc": amount}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Grant", f"Added **{amount:,}** {E_PC} to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="sellpokemon", aliases=["listitem"], description="List Pokemon on User Market.")
async def sellpokemon(ctx, price: int):
    await ctx.send(embed=create_embed(f"{E_PC} Listing", f"Please run `<@716390085896962058> info` now.\nPrice: {price:,} {E_PC} (Tax: 5%)", 0x3498db))
    
    def check(m): 
        return m.channel == ctx.channel and m.author.id == 716390085896962058 and m.embeds

    try:
        msg = await bot.wait_for('message', check=check, timeout=30)
        embed = msg.embeds[0]
        
        # Scrape Info
        desc = embed.description if embed.description else ""
        title = embed.title if embed.title else "Unknown"
        
        # Simple extraction logic (can be refined based on Poketwo actual format)
        level_match = re.search(r"Level\s*(\d+)", desc)
        iv_match = re.search(r"IV\s*[:\s]+([\d\.]+)", desc)
        
        level = int(level_match.group(1)) if level_match else 0
        iv = float(iv_match.group(1)) if iv_match else 0.0
        
        image_url = embed.thumbnail.url if embed.thumbnail else (embed.image.url if embed.image else None)
        
        item_id = f"U{get_next_id('shop_item_id')}"
        shop_items_col.insert_one({
            "id": item_id, "type": "pokemon", "name": title, "price": price, 
            "currency": "pc", "seller_id": str(ctx.author.id), "image_url": image_url,
            "stats": {"level": level, "iv": iv}, "sold": False, "tax_exempt": False, 
            "category": "user_market", "timestamp": datetime.now()
        })
        
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Listed", f"**{title}** listed for **{price:,}** {E_PC}.\nID: {item_id}", 0x2ecc71, thumbnail=image_url))
        
    except asyncio.TimeoutError:
        await ctx.send(embed=create_embed("Timeout", "Listing cancelled.", 0xff0000))
    except Exception as e:
        await ctx.send(embed=create_embed("Error", f"Could not parse info: {str(e)}", 0xff0000))
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

@bot.hybrid_command(name="shop", description="Open Shop Menu.")
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

@bot.hybrid_command(name="pinfo", description="Inspect Admin Pokemon.")
async def pinfo(ctx, item_id: str):
    item = shop_items_col.find_one({"id": item_id, "type": "pokemon", "seller_id": "ADMIN"})
    if not item: return await ctx.send(embed=create_embed("Not Found", "Invalid ID or not an Admin Pokemon.", 0xff0000))
        
    stats = item.get("stats", {"level": 0, "iv": 0})
    cat_raw = item.get("sub_category", "Unknown")
    emoji_map = {"common": "🍀", "rare": "🌌", "shiny": "✨", "regional": "⛩️"}
    
    desc = (
        f"**Name:** {item['name']}\n"
        f"**Category:** {cat_raw.title()} {emoji_map.get(cat_raw, '')}\n"
        f"**Level:** {stats['level']}\n"
        f"**IV:** {stats['iv']}%\n"
        f"**Price:** {item['price']:,} {E_SHINY}\n"
        f"**Status:** {'Sold 🔴' if item['sold'] else 'Available 🟢'}"
    )
    await ctx.send(embed=create_embed(f"{E_PC} Pokemon Inspection", desc, 0x3498db, thumbnail=item.get("image_url")))


@bot.hybrid_command(name="inventory", aliases=["inv"], description="View Items & Balance.")
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

@bot.hybrid_command(name="botinfo", aliases=["info", "guide"], description="Open help panel.")
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
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (Build v5.8)")
    try: await bot.tree.sync()
    except Exception as e: print(f"Tree Sync Error: {e}")
    if not hasattr(bot, 'started'):
        bot.loop.create_task(market_simulation_task())
        bot.started = True

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound): pass
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send(embed=create_embed("Error", f"Missing Argument: {error.param}", 0xff0000))
    elif isinstance(error, commands.MissingPermissions): await ctx.send(embed=create_embed("Error", "No Permission.", 0xff0000))
    elif isinstance(error, commands.BadArgument): await ctx.send(embed=create_embed("Error", str(error), 0xff0000))
    else: print(error)

if __name__ == "__main__":

    bot.run(DISCORD_TOKEN)































