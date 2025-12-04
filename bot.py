# bot.py
# Full Club Auction & Pokémon Shop Bot (Certified Final Production Build v5.1)
# Part 1 of 2 - Core, Economy & Club Market
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
    "shop_log": 1446017729340379246, 
    "shop_main": 1446018190093058222,
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
DAILY_MSG_REQ = 100
POKETWO_ID = 716390085896962058

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
MYSTERY_CONFIG = {
    "common": {"price": 800, "name": "Common Box", "emoji": "📦"},
    "rare": {"price": 3500, "name": "Rare Box", "emoji": "🎁"},
    "shiny": {"price": 7500, "name": "Shiny Box", "emoji": "✨"},
    "regional": {"price": 12000, "name": "Regional Box", "emoji": "🌏"}
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
    pending_deals_col = db.pending_deals
    shop_items_col = db.shop_items
    inventory_col = db.inventory
    coupons_col = db.coupons
    redeem_codes_col = db.redeem_codes
    message_counts_col = db.message_counts

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
    # Check for Specific Chat Channel (ID: 975275349573271552)
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
    log_past_entity(ctx.author.id, "ex_member", gname)
    log_user_activity(ctx.author.id, "Group", f"Left group {name}.")
    await ctx.send(embed=create_embed(f"{E_DANGER} Left Group", f"Left **{name}**. Penalty: **${penalty:,}**.", 0xff0000))

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
#   GROUP 3: FOOTBALL FEATURES
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
    embed = discord.Embed(title=f"{E_CROWN} {c['name']}", description=f"{E_BOOST} **{c.get('level_name')}**", color=0x3498db)
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
    embed_log = create_embed(f"{E_FIRE} Match Result", f"{resolve_emoji(E_WINNER_TROPHY)} **Winner:** {wc['name']} {winner_emoji}\n{resolve_emoji(E_LOSER_MARK)} **Loser:** {lc['name']} {loser_emoji}\n\n_{final_banter}_", 0xe74c3c)
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

@bot.hybrid_command(name="checkdeals", aliases=["cd"], description="Admin: View pending deals.")
@commands.has_permissions(administrator=True)
async def checkdeals(ctx):
    deals = list(pending_deals_col.find().sort("timestamp", 1))
    if not deals: return await ctx.send(embed=create_embed(f"{E_SUCCESS} All Clear", "No pending deals.", 0x2ecc71))
    data = []
    for d in deals:
        buyer_display = d['buyer_id'].replace("group:", "Group: ").title() if "group:" in d['buyer_id'] else f"<@{d['buyer_id']}>"
        data.append((f"Deal #{d['id']} | {d['club_name']}", f"{E_MONEY} **Price:** ${d['price']:,}\n{E_CROWN} **Buyer:** {buyer_display}"))
    view = Paginator(ctx, data, f"{E_ADMIN} Pending Approvals", 0xe67e22, 5)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="managedeal", aliases=["md"], description="Admin: Approve/Reject deal.")
@commands.has_permissions(administrator=True)
async def managedeal(ctx, deal_id: int, action: str):
    action = action.lower()
    if action not in ["approve", "reject"]: return await ctx.send(embed=create_embed("Error", "Action must be `approve` or `reject`.", 0xff0000))
    deal = pending_deals_col.find_one({"id": deal_id})
    if not deal: return await ctx.send(embed=create_embed("Error", "Deal ID not found.", 0xff0000))
    c = clubs_col.find_one({"id": deal['club_id']})
    buyer_id = deal['buyer_id']
    price = deal['price']
    if action == "reject":
        if deal['type'] == "group":
            gname = buyer_id.replace("group:", "")
            groups_col.update_one({"name": gname}, {"$inc": {"funds": price}})
        else:
            wallets_col.update_one({"user_id": buyer_id}, {"$inc": {"balance": price}})
        pending_deals_col.delete_one({"id": deal_id})
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Rejected", f"Deal #{deal_id} rejected. Funds refunded.", 0x2ecc71))
        return
    if action == "approve":
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": buyer_id}})
        if deal['type'] == "user":
            profiles_col.update_one({"user_id": buyer_id}, {"$set": {"owned_club_id": c["id"], "owned_club_share": 100}}, upsert=True)
        history_col.insert_one({"club_id": c["id"], "winner": buyer_id, "amount": price, "timestamp": datetime.now(), "type": "market_buy"})
        pending_deals_col.delete_one({"id": deal_id})
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Approved", f"Deal #{deal_id} approved.", 0x2ecc71))

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
    final_winners = random.sample(users, min(len(users), winners_count))
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
#   GROUP 6: INTERACTIVE SHOP & INFO
# ===========================

class ShopSelect(discord.ui.Select):
    def __init__(self, shop_type):
        self.shop_type = shop_type
        options = [
            discord.SelectOption(label="Shiny Pokémon", emoji="✨", value="shiny"),
            discord.SelectOption(label="Rare Pokémon", emoji="🎁", value="rare"),
            discord.SelectOption(label="Regional Pokémon", emoji="🌏", value="regional"),
            discord.SelectOption(label="Common Pokémon", emoji="📦", value="common"),
        ]
        super().__init__(placeholder="Select a Category...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        category = self.values[0]
        currency = "shiny" if self.shop_type == "admin" else "pc"
        emoji_c = E_SHINY if currency == "shiny" else E_PC
        
        items = list(shop_items_col.find({"currency": currency, "sold": False, "category": category}))
        
        if not items:
            return await interaction.response.send_message(f"No items found in **{category.title()}**.", ephemeral=True)
            
        data = []
        for i in items:
            # ID Prefix: A = Admin, U = User
            prefix = "A" if self.shop_type == "admin" else "U"
            display_id = f"{prefix}{i['id']}"
            
            shiny_tag = "✨ " if i.get('is_shiny') else ""
            data.append((f"{shiny_tag}{i['name']}", f"**Price:** {i['price']:,} {emoji_c}\n**ID:** `{display_id}`"))
            
        view = Paginator(interaction.client.get_context(interaction.message), data, f"{self.shop_type.title()} Shop - {category.title()}", 0x3498db)
        await interaction.response.send_message(embed=view.get_embed(), view=view, ephemeral=True)

class ShopView(discord.ui.View):
    def __init__(self, ctx):
        super().__init__(timeout=60)
        self.ctx = ctx

    @discord.ui.button(label="Admin Shop", style=discord.ButtonStyle.danger, emoji=discord.PartialEmoji.from_str(E_SHINY))
    async def admin_shop(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.ctx.author.id: return
        view = View()
        view.add_item(ShopSelect("admin"))
        await interaction.response.send_message(embed=create_embed(f"{E_SHINY} Admin Shop", "Select a category below:", 0xe74c3c), view=view, ephemeral=True)

    @discord.ui.button(label="User Shop", style=discord.ButtonStyle.success, emoji=discord.PartialEmoji.from_str(E_PC))
    async def user_shop(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.ctx.author.id: return
        view = View()
        view.add_item(ShopSelect("user"))
        await interaction.response.send_message(embed=create_embed(f"{E_PC} User Shop", "Select a category below:", 0x2ecc71), view=view, ephemeral=True)

@bot.hybrid_command(name="shop", description="Open the Pokémon Shop.")
async def shop(ctx):
    view = ShopView(ctx)
    await ctx.send(embed=create_embed(f"{E_ITEMBOX} Pokémon Market", "Welcome! Select a shop type:", 0x3498db), view=view)

@bot.hybrid_command(name="shop_import", aliases=["si"], description="Admin: Add item (Cat: shiny/rare/regional/common).")
@commands.has_permissions(administrator=True)
async def shop_import(ctx, name: str, price: int, category: str, is_shiny: bool = False, image: discord.Attachment = None):
    cat = category.lower()
    if cat not in ['shiny', 'rare', 'regional', 'common']: return await ctx.send("Invalid category. Use: shiny, rare, regional, common")
    
    item_id = get_next_id("shop_item_id")
    image_url = image.url if image else None
    
    shop_items_col.insert_one({
        "id": item_id,
        "name": name,
        "price": price,
        "currency": "shiny",
        "seller_id": "ADMIN",
        "is_shiny": is_shiny,
        "image_url": image_url,
        "sold": False,
        "timestamp": datetime.now(),
        "category": cat
    })
    
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Added", f"**{name}** ({cat}) added to Admin Shop.\nPrice: {price:,} {E_SHINY}\nID: `A{item_id}`", 0x2ecc71, image=image_url))

@bot.hybrid_command(name="user_shop_add", aliases=["usa"], description="List item (Cat: shiny/rare/regional/common).")
async def user_shop_add(ctx, price: int, category: str):
    cat = category.lower()
    if cat not in ['shiny', 'rare', 'regional', 'common']: return await ctx.send("Invalid category.")

    await ctx.send(embed=create_embed(f"{E_PC} List Item", f"**Step 1:** Price: {price:,} {E_PC} | Category: {cat}\n**Step 2:** Run `<@716390085896962058> info` now.", 0x3498db))

    def check(m): return m.channel == ctx.channel and m.author.id == POKETWO_ID and m.embeds
    try:
        msg = await bot.wait_for('message', check=check, timeout=30)
        embed = msg.embeds[0]
        name = embed.title if embed.title else "Unknown"
        image_url = embed.thumbnail.url if embed.thumbnail else (embed.image.url if embed.image else None)
        
        item_id = get_next_id("shop_item_id")
        shop_items_col.insert_one({
            "id": item_id,
            "name": name,
            "price": price,
            "currency": "pc",
            "seller_id": str(ctx.author.id),
            "is_shiny": False, # Cannot verify shiny status easily from embed title alone without regex
            "image_url": image_url,
            "sold": False,
            "timestamp": datetime.now(),
            "category": cat
        })
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Listed", f"**{name}** listed in {cat} for {price:,} {E_PC}!\nID: `U{item_id}`", 0x2ecc71, image=image_url))
    except asyncio.TimeoutError: await ctx.send(embed=create_embed("Timeout", "No info message received.", 0xff0000))

@bot.hybrid_command(name="shop_buy", aliases=["sb"], description="Buy item (ID: A123 or U123).")
async def shop_buy(ctx, item_id_str: str):
    # Parse ID prefix
    prefix = item_id_str[0].upper()
    try:
        item_id = int(item_id_str[1:])
    except:
        return await ctx.send(embed=create_embed("Error", "Invalid ID format. Use A123 or U123.", 0xff0000))

    # Verify shop type matches ID
    # Admin items have seller_id = "ADMIN", User items have discord ID string
    item = shop_items_col.find_one({"id": item_id, "sold": False})
    
    if not item: return await ctx.send(embed=create_embed("Error", "Item not found or sold.", 0xff0000))

    # Check if ID matches shop type
    is_admin_item = (item['seller_id'] == "ADMIN")
    if (prefix == "A" and not is_admin_item) or (prefix == "U" and is_admin_item):
        return await ctx.send(embed=create_embed("Error", f"ID mismatch. This item is in the {'Admin' if is_admin_item else 'User'} shop.", 0xff0000))

    # Payment Logic
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    currency_field = "shiny_coins" if item['currency'] == "shiny" else "pc"
    balance = w.get(currency_field, 0) if w else 0
    
    if balance < item['price']: return await ctx.send(embed=create_embed("Insufficient Funds", f"Need **{item['price']:,}** but have **{balance:,}**.", 0xff0000))
    
    # Pending Deal for User Shop? Or Instant? 
    # Prompt asked for Pending Deal System for Pokemon too.
    
    # Create Pending Deal
    deal_id = get_next_id("deal_id")
    pending_deals_col.insert_one({
        "id": deal_id,
        "type": "pokemon", # Differentiator
        "buyer_id": str(ctx.author.id),
        "item_id": item_id,
        "item_name": item['name'],
        "price": item['price'],
        "currency": currency_field,
        "seller_id": item['seller_id'],
        "timestamp": datetime.now()
    })
    
    # Deduct Money to Hold in Escrow
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {currency_field: -item['price']}})
    
    await ctx.send(embed=create_embed(f"{E_TIMER} Deal Pending", f"Request to buy **{item['name']}** submitted.\nFunds Held. Waiting for Admin Approval.\nDeal ID: **{deal_id}**", 0xf1c40f))

@bot.hybrid_command(name="pc_deposit", aliases=["pcd"], description="Deposit PC from PokéTwo.")
async def pc_deposit(ctx):
    await ctx.send(embed=create_embed(f"{E_PC} PC Deposit", "Please run `<@716390085896962058> bal` in this channel now.", 0x3498db))
    
    def check(m):
        if m.channel.id != ctx.channel.id or m.author.id != POKETWO_ID: return False
        is_related = (str(ctx.author.id) in m.content) or (m.reference is not None) or (len(m.embeds) > 0)
        return is_related

    try:
        msg = await bot.wait_for('message', check=check, timeout=30)
        
        full_text = msg.content.lower()
        if msg.embeds:
            e = msg.embeds[0]
            full_text += f" {e.title or ''} {e.description or ''}"
            for f in e.fields: full_text += f" {f.name} {f.value}"
            
        full_text = full_text.replace(",", "")
        match = re.search(r'(\d+)\s*pc', full_text)
        
        if not match: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Could not read balance.", 0xff0000))
        
        poketwo_bal = int(match.group(1))
        await ctx.send(embed=create_embed(f"{E_PC} Deposit Request", f"Found **{poketwo_bal:,} PC**. How much to deposit?", 0x3498db))
        
        def amt_check(m): return m.author == ctx.author and m.channel == ctx.channel and m.content.replace(",", "").isdigit()
        amt_msg = await bot.wait_for('message', check=amt_check, timeout=30)
        amount = int(amt_msg.content.replace(",", ""))
        
        if amount > poketwo_bal: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Not enough PC.", 0xff0000))
            
        # Deposit View Class defined earlier in Part 8 logic (reused here inline if needed or assumed defined)
        # For safety in this Part 2 file, defining simple View here
        class ConfirmView(discord.ui.View):
             def __init__(self): super().__init__(timeout=60); self.value = None
             @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
             async def c(self, i, b): self.value = True; self.stop()
             @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
             async def ca(self, i, b): self.value = False; self.stop()

        view = ConfirmView()
        conf_msg = await ctx.send(embed=create_embed(f"{E_ALERT} Confirmation", f"Deposit **{amount:,} PC**?", 0xf1c40f), view=view)
        await view.wait()
        
        if view.value:
            wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"pc": amount}}, upsert=True)
            await conf_msg.edit(embed=create_embed(f"{E_SUCCESS} Successful", f"Added **{amount:,} {E_PC}**.", 0x2ecc71), view=None)
        else: 
            await conf_msg.edit(embed=create_embed("Cancelled", "Deposit cancelled.", 0x95a5a6), view=None)
            
    except asyncio.TimeoutError: 
        await ctx.send(embed=create_embed("Timeout", "Session expired.", 0xff0000))

@bot.hybrid_command(name="daily", aliases=["claim"], description="Claim daily ($10k). Req: 100 msgs.")
async def daily(ctx):
    today_str = datetime.now().strftime("%Y-%m-%d")
    msg_data = message_counts_col.find_one({"user_id": str(ctx.author.id), "date": today_str})
    msg_count = msg_data.get("count", 0) if msg_data else 0

    if msg_count < DAILY_MSG_REQ:
        return await ctx.send(embed=create_embed(f"{E_ERROR} Not Qualified", f"You need **{DAILY_MSG_REQ} messages** in the chat channel today.\n**Current:** {msg_count}/{DAILY_MSG_REQ}", 0xff0000))

    user = wallets_col.find_one({"user_id": str(ctx.author.id)})
    last = user.get("last_daily")
    if last and (datetime.now() - last) < timedelta(hours=24): return await ctx.send(embed=create_embed("Cooldown", "Come back later!", 0xff0000))
    
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": 10000}, "$set": {"last_daily": datetime.now()}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_GIVEAWAY} Daily Claimed", f"+$10,000 {E_MONEY}", 0x2ecc71))

class HelpView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def send_category_embed(self, interaction, title, description, fields, color):
        embed = discord.Embed(title=title, description=description, color=color)
        for name, val in fields:
            embed.add_field(name=name, value=val, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Economy & Groups", style=discord.ButtonStyle.success, emoji=discord.PartialEmoji.from_str(E_MONEY))
    async def economy_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_MONEY} **Personal Finance**", 
             f"`{.wl}` **Wallet:** Check balances.\n"
             f"`{.ww <Amt>}` **Withdraw Wallet:** Burn/delete money."),
            (f"{E_PREMIUM} **Investment Groups**", 
             f"`{.cg <Name> <%>}` **Create Group**\n"
             f"`{.jg <Name> <%>}` **Join Group**\n"
             f"`{.gi <Name>}` **Group Info**\n"
             f"`{.gl}` **Group List**\n"
             f"`{.lg <Name>}` **Leave Group**"),
            (f"{E_BOOST} **Banking Actions**", 
             f"`{.dep <Grp> <Amt>}` **Deposit:** Wallet -> Group\n"
             f"`{.wd <Grp> <Amt>}` **Withdraw:** Group -> Wallet")
        ]
        await self.send_category_embed(interaction, f"{E_MONEY} Economy & Groups", "Manage finances.", fields, 0x2ecc71)

    @discord.ui.button(label="Football & Roles", style=discord.ButtonStyle.primary, emoji=discord.PartialEmoji.from_str(E_FIRE))
    async def football_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_CROWN} **Club Stats**", f"`{.ci <Club>}` Info, `{.cl <Club>}` Level, `{.lc}` List, `{.lb}` Leaderboard."),
            (f"{E_ITEMBOX} **Duelists**", f"`{.rd}` Register, `{.ld}` List, `{.ret}` Retire."),
            (f"{E_ADMIN} **Owner Tools**", f"`{.as <ID> <Amt>}` Salary, `{.ds <ID> yes}` Deduct.")
        ]
        await self.send_category_embed(interaction, f"{E_STAR} Football Features", "Manage clubs.", fields, 0x3498db)

    @discord.ui.button(label="Club Market", style=discord.ButtonStyle.secondary, emoji=discord.PartialEmoji.from_str(E_AUCTION))
    async def market_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_AUCTION} **Trading**", f"`{.ml}` Market List, `.bc` Buy Club, `.sc` Sell Club."),
            (f"{E_TIMER} **Auctions**", f"`{.pb}` Place Bid, `.gb` Group Bid.")
        ]
        await self.send_category_embed(interaction, f"{E_AUCTION} Club Market", "Trading hub.", fields, 0xe67e22)

    @discord.ui.button(label="Pokémon Shop", style=discord.ButtonStyle.secondary, emoji=discord.PartialEmoji.from_str(E_PC))
    async def shop_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_SHINY} **Admin Shop**", f"`.shop` Open Menu.\n`.sb <ID>` Buy Item."),
            (f"{E_PC} **User Shop**", f"`.usa <Price> <Cat>` List Item.\n`.usr <ID>` Remove Listing."),
            (f"{E_ITEMBOX} **Rewards**", f"`.pcd` Deposit PC.\n`.daily` Claim Reward.\n`.mb` Mystery Box.")
        ]
        await self.send_category_embed(interaction, f"{E_PC} Pokémon Market", "Buy & Sell.", fields, 0x9b59b6)

    @discord.ui.button(label="Admin Tools", style=discord.ButtonStyle.danger, emoji=discord.PartialEmoji.from_str(E_ADMIN))
    async def admin_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message(f"{E_ERROR} Staff Access Only.", ephemeral=True)
        fields = [
            (f"{E_ADMIN} **Management**", f"`.cd` Check Deals, `.md` Manage Deal.\n`.rc` Register Club, `.dc` Delete Club.\n`.si` Shop Import, `.sr` Shop Remove."),
            (f"{E_AUCTION} **Auctions**", f"`.sca` Start Club, `.sda` Start Duelist.\n`.fa` Freeze, `.ufa` Unfreeze."),
            (f"{E_MONEY} **Economy**", f"`.tp` Tip, `.du` Deduct, `.po` Payout.")
        ]
        await self.send_category_embed(interaction, f"{E_ADMIN} Staff Commands", "Control panel.", fields, 0xff0000)

    @discord.ui.button(label="Updates (v5.0)", style=discord.ButtonStyle.primary, emoji=discord.PartialEmoji.from_str(E_BOOST))
    async def updates_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        desc = (
            f"{E_STARS} **Latest Version 5.0**\n\n"
            f"{E_GOLD_TICK} **Interactive Shop:** Dropdown Categories.\n"
            f"{E_GOLD_TICK} **Alphanumeric IDs:** A1 (Admin), U1 (User).\n"
            f"{E_GOLD_TICK} **Pending Deals:** Both Clubs & Pokémon require approval.\n"
            f"{E_GOLD_TICK} **Daily Task:** 100 Msgs in Chat Channel.\n"
            f"{E_GOLD_TICK} **PC Deposit:** Fixed & Secured."
        )
        await self.send_category_embed(interaction, f"{E_ALERT} Patch Notes", desc, [], 0x9b59b6)

@bot.hybrid_command(name="botinfo", aliases=["info", "guide"], description="Open the main help and information panel.")
async def botinfo(ctx):
    embed = discord.Embed(
        title=f"{E_CROWN} **Ze Bot: Information & Guide**",
        description=(
            f"**Welcome to the ultimate Football Economy simulation!**\n\n"
            f"**{E_STAR} Why is this bot here?**\n"
            f"Built for this server to bring the thrill of the **Transfer Market** and **Club Ownership** to Discord. "
            f"Simulate the life of a wealthy investor or manager.\n\n"
            f"**{E_SHINY} Core Purpose**\n"
            f"• **Economy:** Earn money, trade assets, and build an empire.\n"
            f"• **Competition:** Buy real clubs, battle for Divisions, and prove who is the best.\n"
            f"• **Events:** Daily giveaways and auctions.\n\n"
            f"**{E_ADMIN} Credits**\n"
            f"Designed and created by **Soul Gill**.\n\n"
            f"**{E_BOOK} Navigation**\n"
            f"Click the buttons below to view the full command list."
        ),
        color=0xf1c40f
    )
    if bot.user.avatar: embed.set_thumbnail(url=bot.user.avatar.url)
    view = HelpView()
    await ctx.send(embed=embed, view=view)

# ---------- RUN ----------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    try: await bot.tree.sync()
    except Exception as e: print(e)
    if not hasattr(bot, 'started'):
        bot.loop.create_task(market_simulation_task())
        bot.started = True

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound): pass
    else: await ctx.send(embed=create_embed("Error", str(error), 0xff0000))

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)