<?php
/**
 * UFAA – Dedicated Web-Excel Data Sanitizer Editor
 * Interactive Web-Excel Editor with live multi-officer presence tracking,
 * interactive card-based filtering & inspection, complete multi-column validation,
 * alphabetically sorted officer avatars, compact inline popovers, and Excel keyboard shortcuts.
 */

require_once 'config.php';

$draftId = isset($_GET['draft_id']) ? intval($_GET['draft_id']) : 0;
if ($draftId <= 0) {
    header('Location: data_preparation.php');
    exit;
}

$pageTitle  = 'Data Sanitizer Workspace';
$activePage = 'data_prep';
require_once 'includes/layout.php';
?>

<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>

<style>
.dp-label { font-size:.72rem; font-weight:700; text-transform:uppercase; letter-spacing:.05em; color:#94a3b8; }

/* ─── Floating Presence Overlay ───────────────────────── */
.table-responsive-wrapper { position:relative; max-height:560px; overflow:auto; }
#presence-overlay-layer   { position:absolute; top:0; left:0; width:100%; height:100%; pointer-events:none; z-index:15; }
.presence-box             { position:absolute; border:2.5px solid #1d4ed8; pointer-events:none; box-sizing:border-box; z-index:16; transition: all 0.15s ease; }
.presence-box-badge {
    position:absolute; top:-11px; right:-4px;
    color:#fff; font-size:.65rem; font-weight:800;
    padding:2px 6px; border-radius:10px;
    box-shadow:0 2px 6px rgba(0,0,0,.25);
    border:1.5px solid #fff; white-space:nowrap; z-index:17;
    cursor:pointer; pointer-events:auto;
}

/* ─── Viewer Avatar Stack (Alphabetically Ordered) ─────── */
.viewer-avatar {
    width:28px; height:28px; border-radius:50%;
    color:#fff; font-size:.68rem; font-weight:800;
    display:inline-flex; align-items:center; justify-content:center;
    border:2px solid #fff; box-shadow:0 1px 4px rgba(0,0,0,.18);
    margin-left:-7px; cursor:pointer; transition:transform 0.15s ease;
    position:relative;
}
.viewer-avatar:first-child { margin-left:0; }
.viewer-avatar:hover { transform:scale(1.15); z-index:10; }

/* ─── Jump Highlight Ring Animation ────────────────────── */
@keyframes jumpPulse {
    0% { outline: 4px solid #3b82f6; background-color: #dbeafe; }
    50% { outline: 6px solid #1d4ed8; background-color: #bfdbfe; }
    100% { outline: 0px solid transparent; background-color: transparent; }
}
.jump-highlight {
    animation: jumpPulse 2.5s ease-out;
}

/* ─── Compact Inline Target-Anchored Popover ───────────── */
#officer-inline-popover {
    display:none; position:absolute; z-index:9995;
    background:#ffffff; border:1px solid #cbd5e1; border-radius:12px;
    padding:0.85rem 1rem; width:250px;
    box-shadow:0 10px 25px -5px rgba(0,0,0,0.2), 0 8px 10px -6px rgba(0,0,0,0.1);
    animation: popoverFadeIn 0.15s ease-out;
}
@keyframes popoverFadeIn {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
}
</style>

<!-- ══════════════════════════════════════════════════════════
     WORKSPACE (Full Page Dedicated Web-Excel Editor)
══════════════════════════════════════════════════════════ -->
<div id="grid-workspace">

    <!-- Flagged Admin Duplicate Review Notice -->
    <div id="flagged-admin-banner-wrap"></div>

    <!-- Workspace Top Bar: File Name + Viewers + Back Button -->
    <div style="background:#fff; border:1px solid #e2e8f0; border-radius:12px; padding:.85rem 1.25rem; margin-bottom:1.1rem; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.75rem;">
        <div style="display:flex; align-items:center; gap:.75rem;">
            <a href="data_preparation.php" class="btn-reset" title="Back to file list" style="text-decoration:none; color:#64748b; font-size:.875rem; font-weight:700; padding:.4rem .75rem; border-radius:8px; border:1px solid #cbd5e1; display:inline-flex; align-items:center; gap:.4rem;">
                <i class="fa-solid fa-arrow-left"></i> Back to File List
            </a>
            <div style="height:24px; width:1px; background:#cbd5e1; margin:0 .2rem;"></div>
            <i class="fa-regular fa-file-excel" style="color:var(--airtel-red); font-size:1.25rem;"></i>
            <div>
                <div id="workspace-filename" style="font-size:.98rem; font-weight:800; color:var(--text-primary); line-height:1.2;">Loading file…</div>
                <div id="workspace-meta" style="font-size:.72rem; color:#64748b;">Auto-saved · Live editing enabled</div>
            </div>
        </div>
        <div style="display:flex; align-items:center; gap:.85rem; flex-wrap:wrap;">
            <!-- Active Viewing Officers (Alphabetical) -->
            <div style="display:flex; align-items:center; gap:.5rem; position:relative;">
                <span style="font-size:.72rem; font-weight:700; color:#94a3b8; text-transform:uppercase; letter-spacing:.05em;">Viewing:</span>
                <div id="viewer-avatar-stack" style="display:flex; align-items:center; min-width:26px;"></div>
            </div>
            <div id="autosave-badge" style="font-size:.78rem; font-weight:600; color:#0369a1; background:#e0f2fe; padding:.28rem .7rem; border-radius:7px; border:1px solid #bae6fd; display:inline-flex; align-items:center; gap:.35rem;">
                <span style="display:inline-block; width:7px; height:7px; background:#0369a1; border-radius:50%;"></span> Live Editing
            </div>
        </div>
    </div>

    <!-- Interactive Summary Cards -->
    <div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr)); gap:.8rem; margin-bottom:1rem;">
        <div id="card-total" style="background:#fff; padding:.85rem 1rem; border-radius:10px; border:2px solid #1d4ed8; cursor:pointer; transition:all 0.15s ease;" onclick="setCardFilter('all')" title="Click to view all rows">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div class="dp-label">Total Rows</div>
                <span class="card-filter-badge" style="font-size:.68rem; font-weight:700; color:#64748b; background:#f1f5f9; padding:1px 6px; border-radius:6px;">All</span>
            </div>
            <div id="summary-total" style="font-size:1.45rem; font-weight:800; color:var(--text-primary);">0</div>
        </div>
        <div id="card-clean" style="background:#fff; padding:.85rem 1rem; border-radius:10px; border:1px solid #e2e8f0; cursor:pointer; transition:all 0.15s ease;" onclick="setCardFilter('clean')" title="Click to filter clean rows only">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div class="dp-label" style="color:#16a34a;">Clean</div>
                <span class="card-filter-badge" style="font-size:.68rem; font-weight:700; color:#16a34a; background:#dcfce7; padding:1px 6px; border-radius:6px;">Filter</span>
            </div>
            <div id="summary-clean" style="font-size:1.45rem; font-weight:800; color:#16a34a;">0</div>
        </div>
        <div id="card-critical" style="background:#fff; padding:.85rem 1rem; border-radius:10px; border:1px solid #e2e8f0; cursor:pointer; transition:all 0.15s ease;" onclick="setCardFilter('critical')" title="Click to filter critical error rows only">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div class="dp-label" style="color:#dc2626;">Errors</div>
                <span class="card-filter-badge" style="font-size:.68rem; font-weight:700; color:#dc2626; background:#fee2e2; padding:1px 6px; border-radius:6px;">Filter</span>
            </div>
            <div id="summary-critical" style="font-size:1.45rem; font-weight:800; color:#dc2626;">0</div>
        </div>
        <div id="card-warnings" style="background:#fff; padding:.85rem 1rem; border-radius:10px; border:1px solid #e2e8f0; cursor:pointer; transition:all 0.15s ease;" onclick="setCardFilter('warnings')" title="Click to filter warning rows only">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div class="dp-label" style="color:#d97706;">Warnings</div>
                <span class="card-filter-badge" style="font-size:.68rem; font-weight:700; color:#d97706; background:#fef3c7; padding:1px 6px; border-radius:6px;">Filter</span>
            </div>
            <div id="summary-warnings" style="font-size:1.45rem; font-weight:800; color:#d97706;">0</div>
        </div>
        <!-- Interactive Duplicates Card -->
        <div id="card-duplicates" style="background:#fff; padding:.85rem 1rem; border-radius:10px; border:1px solid #e2e8f0; cursor:pointer; transition:all 0.15s ease;" onclick="setCardFilter('duplicates')" title="Click to filter duplicate rows only">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div class="dp-label" style="color:#6b21a8;">Duplicates</div>
                <span class="card-filter-badge" style="font-size:.68rem; font-weight:700; color:#6b21a8; background:#f3e8ff; padding:1px 6px; border-radius:6px;">Filter</span>
            </div>
            <div id="summary-duplicates" style="font-size:1.45rem; font-weight:800; color:#6b21a8;">0</div>
        </div>
    </div>

    <!-- Toolbar -->
    <div style="background:#fff; padding:.8rem 1.1rem; border-radius:10px; border:1px solid #e2e8f0; margin-bottom:1rem; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.6rem;">
        <div style="display:flex; gap:.5rem; align-items:center; flex-wrap:wrap;">
            <button class="btn-reset" onclick="triggerUndo()" title="Undo last edit (Ctrl + Z)" style="padding:.42rem .75rem; font-size:.82rem; border-radius:7px;">
                <i class="fa-solid fa-rotate-left"></i> Undo (Ctrl+Z)
            </button>
            <button class="btn-reset" onclick="triggerRedo()" title="Redo last edit (Ctrl + Y)" style="padding:.42rem .75rem; font-size:.82rem; border-radius:7px;">
                <i class="fa-solid fa-rotate-right"></i> Redo (Ctrl+Y)
            </button>
            <button class="btn-reset" onclick="exportDraftExcel()" style="padding:.42rem .85rem; font-size:.82rem; border-radius:7px;">
                <i class="fa-solid fa-file-export" style="color:#0369a1;"></i> Export Draft
            </button>
            <button class="btn-reset" onclick="exportFilteredExcel()" id="btn-export-filter" style="padding:.42rem .85rem; font-size:.82rem; border-radius:7px; color:#0369a1; background:#e0f2fe; border:1px solid #bae6fd;">
                <i class="fa-solid fa-filter"></i> Export Filter
            </button>
            <button class="btn-reset" onclick="autoFixAllCompilationDates()" style="padding:.42rem .85rem; font-size:.82rem; border-radius:7px;">
                <i class="fa-solid fa-wand-magic-sparkles" style="color:#6d28d9;"></i> Auto-Fix Dates
            </button>
            <button class="btn-reset" onclick="openDuplicateInspectorModal()" style="padding:.42rem .85rem; font-size:.82rem; border-radius:7px; color:#4338ca; background:#f5f3ff; border:1px solid #ddd6fe;">
                <i class="fa-solid fa-copy"></i> Inspect Duplicates
            </button>
        </div>
        <button id="btn-submit-locked" class="btn-filter" onclick="commitSanitizedUpload()"
            style="padding:.52rem 1.25rem; font-size:.875rem; border-radius:8px; display:inline-flex; align-items:center; gap:.5rem; background:#16a34a; color:#fff; cursor:pointer; transition:all .2s;">
            <i class="fa-solid fa-cloud-arrow-up" id="btn-submit-icon"></i>
            <span id="btn-submit-text">Upload &amp; Lock File</span>
        </button>
    </div>

    <!-- Grid Table -->
    <div style="background:#fff; border:1px solid #e2e8f0; border-radius:12px; overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.04);">
        <!-- Top Toolbar & Status -->
        <div style="background:#f8fafc; padding:.65rem 1.1rem; border-bottom:1px solid #e2e8f0; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.5rem;">
            <div style="display:flex; align-items:center; gap:.65rem; flex-wrap:wrap;">
                <span style="font-size:.84rem; font-weight:800; color:var(--text-secondary); display:flex; align-items:center; gap:.5rem;">
                    <i class="fa-solid fa-table-cells"></i> Editable Grid
                </span>
                <span id="active-filter-indicator" style="font-size:.72rem; font-weight:700; color:#1d4ed8; background:#dbeafe; padding:2px 8px; border-radius:10px;">Showing All Rows</span>
                <span id="pagination-top-summary" style="font-size:.78rem; font-weight:700; color:#475569; background:#f1f5f9; padding:2px 10px; border-radius:8px; border:1px solid #cbd5e1;">Loading rows…</span>
            </div>
            <div style="display:flex; align-items:center; gap:.75rem;">
                <div id="grid-status" style="font-size:.75rem; font-weight:600; color:#64748b;">Ready</div>
            </div>
        </div>

        <!-- Top Pagination Bar -->
        <div style="background:#ffffff; padding:.55rem 1.1rem; border-bottom:1px solid #e2e8f0; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.65rem;">
            <!-- Page Size Selector -->
            <div style="display:flex; align-items:center; gap:.5rem;">
                <label for="select-page-size" style="font-size:.75rem; font-weight:700; color:#64748b; text-transform:uppercase; letter-spacing:.04em;">Rows / Page:</label>
                <select id="select-page-size" onchange="changePageSize(this.value)" style="padding:.28rem .6rem; border-radius:7px; border:1.5px solid #cbd5e1; font-size:.8rem; font-weight:700; color:#1e293b; background:#f8fafc; cursor:pointer;">
                    <option value="50000" selected>50,000 rows (Recommended)</option>
                    <option value="25000">25,000 rows</option>
                    <option value="10000">10,000 rows</option>
                    <option value="5000">5,000 rows</option>
                    <option value="1000">1,000 rows</option>
                    <option value="500">500 rows</option>
                </select>
                <span style="font-size:.73rem; color:#94a3b8;">· Scrollbar enabled for each page</span>
            </div>

            <!-- Page Navigation Controls -->
            <div style="display:flex; align-items:center; gap:.4rem; flex-wrap:wrap;">
                <button id="btn-page-first" onclick="goToPage(1)" class="btn-reset" title="First Page (Page 1)" style="padding:.3rem .55rem; font-size:.78rem; border-radius:6px; border:1px solid #cbd5e1; color:#334155; font-weight:700;">
                    <i class="fa-solid fa-angles-left"></i> First
                </button>
                <button id="btn-page-prev" onclick="goToPage(currentPage - 1)" class="btn-reset" title="Previous Page" style="padding:.3rem .6rem; font-size:.78rem; border-radius:6px; border:1px solid #cbd5e1; color:#334155; font-weight:700;">
                    <i class="fa-solid fa-chevron-left"></i> Prev
                </button>
                
                <div style="display:flex; align-items:center; gap:.35rem; margin:0 .2rem;">
                    <span style="font-size:.76rem; font-weight:700; color:#64748b;">Page</span>
                    <select id="select-current-page" onchange="goToPage(parseInt(this.value))" style="padding:.28rem .55rem; border-radius:7px; border:1.5px solid #3b82f6; font-size:.8rem; font-weight:800; color:#1d4ed8; background:#eff6ff; cursor:pointer;">
                        <option value="1">Page 1</option>
                    </select>
                    <span id="page-total-label" style="font-size:.76rem; font-weight:700; color:#64748b;">of 1</span>
                </div>

                <button id="btn-page-next" onclick="goToPage(currentPage + 1)" class="btn-reset" title="Next Page" style="padding:.3rem .6rem; font-size:.78rem; border-radius:6px; border:1px solid #cbd5e1; color:#334155; font-weight:700;">
                    Next <i class="fa-solid fa-chevron-right"></i>
                </button>
                <button id="btn-page-last" onclick="goToPage(totalPages)" class="btn-reset" title="Last Page" style="padding:.3rem .55rem; font-size:.78rem; border-radius:6px; border:1px solid #cbd5e1; color:#334155; font-weight:700;">
                    Last <i class="fa-solid fa-angles-right"></i>
                </button>
            </div>
        </div>

        <!-- Scrollable Table Wrapper (Scrollbar for each page with virtual rendering) -->
        <div class="table-responsive-wrapper" id="table-scroll-wrap" onscroll="handleTableScroll()" style="position:relative; max-height:580px; overflow-y:scroll; overflow-x:auto; background:#ffffff;">
            <div id="presence-overlay-layer"></div>
            <table id="spreadsheet-table" style="width:100%; border-collapse:collapse; font-size:.84rem;">
                <thead>
                    <tr style="background:#0f172a; color:#fff; font-weight:700; position:sticky; top:0; z-index:10; box-shadow:0 2px 4px rgba(0,0,0,0.1);">
                        <th style="padding:.65rem .5rem; width:45px; text-align:center; border-right:1px solid #1e293b;">#</th>
                        <th style="padding:.65rem .75rem; min-width:155px; border-right:1px solid #1e293b;">Owner Name *</th>
                        <th style="padding:.65rem .75rem; min-width:125px; border-right:1px solid #1e293b;">ID / Passport No</th>
                        <th style="padding:.65rem .75rem; min-width:110px; border-right:1px solid #1e293b;">Date of Birth</th>
                        <th style="padding:.65rem .75rem; min-width:135px; border-right:1px solid #1e293b;">Account Number *</th>
                        <th style="padding:.65rem .75rem; min-width:140px; border-right:1px solid #1e293b;">Last Transaction</th>
                        <th style="padding:.65rem .75rem; min-width:120px; text-align:right; border-right:1px solid #1e293b;">Due Amount (KES) *</th>
                        <th style="padding:.65rem .75rem; min-width:135px; border-right:1px solid #1e293b;">Compilation Date *</th>
                        <th style="padding:.65rem .75rem; width:55px; text-align:center;">Del</th>
                    </tr>
                </thead>
                <tbody id="spreadsheet-tbody"></tbody>
            </table>
        </div>

        <!-- Bottom Pagination Bar -->
        <div style="background:#f8fafc; padding:.55rem 1.1rem; border-top:1px solid #e2e8f0; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.65rem;">
            <div style="display:flex; align-items:center; gap:.5rem;">
                <span id="pagination-bottom-summary" style="font-size:.78rem; font-weight:700; color:#475569;">Showing 0 – 0 of 0</span>
                <span style="font-size:.72rem; color:#94a3b8;">(Fast virtual scrolling active)</span>
            </div>
            <div style="display:flex; align-items:center; gap:.4rem;">
                <button onclick="goToPage(1)" class="btn-reset" title="First Page" style="padding:.28rem .55rem; font-size:.75rem; border-radius:6px; border:1px solid #cbd5e1; color:#475569; font-weight:700;">
                    <i class="fa-solid fa-angles-left"></i> First
                </button>
                <button onclick="goToPage(currentPage - 1)" class="btn-reset" title="Previous Page" style="padding:.28rem .55rem; font-size:.75rem; border-radius:6px; border:1px solid #cbd5e1; color:#475569; font-weight:700;">
                    <i class="fa-solid fa-chevron-left"></i> Prev
                </button>
                <span id="pagination-bottom-page-badge" style="font-size:.76rem; font-weight:800; color:#1d4ed8; background:#eff6ff; padding:3px 10px; border-radius:6px; border:1px solid #bfdbfe;">
                    Page 1 / 1
                </span>
                <button onclick="goToPage(currentPage + 1)" class="btn-reset" title="Next Page" style="padding:.28rem .55rem; font-size:.75rem; border-radius:6px; border:1px solid #cbd5e1; color:#475569; font-weight:700;">
                    Next <i class="fa-solid fa-chevron-right"></i>
                </button>
                <button onclick="goToPage(totalPages)" class="btn-reset" title="Last Page" style="padding:.28rem .55rem; font-size:.75rem; border-radius:6px; border:1px solid #cbd5e1; color:#475569; font-weight:700;">
                    Last <i class="fa-solid fa-angles-right"></i>
                </button>
            </div>
        </div>
    </div>

</div>

<!-- ══════════════════════════════════════════════════════════
     COMPACT INLINE POPOVER (Anchored next to Avatar/Cell)
══════════════════════════════════════════════════════════ -->
<div id="officer-inline-popover">
    <div style="display:flex; align-items:center; gap:.65rem; margin-bottom:.5rem;">
        <div id="popover-avatar" style="width:32px; height:32px; border-radius:50%; background:#1d4ed8; color:#fff; font-size:.78rem; font-weight:800; display:flex; align-items:center; justify-content:center; flex-shrink:0;">JO</div>
        <div style="min-width:0; flex:1;">
            <div id="popover-name" style="font-size:.875rem; font-weight:800; color:var(--text-primary); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">Officer Name</div>
            <div id="popover-status" style="font-size:.72rem; color:var(--text-secondary);">Viewing draft</div>
        </div>
        <button onclick="closeOfficerPopover()" class="btn-reset" style="font-size:.75rem; color:#94a3b8; padding:2px 5px;"><i class="fa-solid fa-xmark"></i></button>
    </div>
    <div id="popover-action-wrap" style="margin-top:.4rem;">
        <!-- Dynamic button injected here -->
    </div>
</div>

<!-- ══════════════════════════════════════════════════════════
     DUPLICATE INSPECTOR MODAL (In-File & MySQL Database Matches)
══════════════════════════════════════════════════════════ -->
<div id="duplicate-inspector-modal" style="display:none; position:fixed; top:0; left:0; width:100vw; height:100vh; background:rgba(15,23,42,.65); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(4px);" onclick="if(event.target===this)closeDuplicateInspectorModal()">
    <div style="background:#fff; width:92%; max-width:820px; max-height:85vh; border-radius:14px; padding:1.5rem; box-shadow:0 25px 50px -12px rgba(0,0,0,.35); display:flex; flex-direction:column;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem; padding-bottom:.75rem; border-bottom:1px solid #e2e8f0;">
            <div>
                <h3 style="font-size:1.15rem; font-weight:800; color:var(--text-primary); margin:0; display:flex; align-items:center; gap:.5rem;">
                    <i class="fa-solid fa-copy" style="color:#4338ca;"></i> Duplicate Records Inspector
                </h3>
                <p style="font-size:.78rem; color:#64748b; margin:.2rem 0 0 0;">Inspect in-file sheet conflicts and historical records already in the MySQL database.</p>
            </div>
            <button onclick="closeDuplicateInspectorModal()" class="btn-reset" style="padding:.3rem .6rem; border-radius:6px; font-size:.85rem;"><i class="fa-solid fa-xmark"></i></button>
        </div>

        <div style="overflow-y:auto; flex:1; padding-right:4px; overscroll-behavior:contain; will-change:scroll-position;" id="duplicate-modal-content">
            <!-- Dynamic duplicate items injected here -->
        </div>

        <div style="margin-top:1rem; pt:.75rem; border-top:1px solid #e2e8f0; text-align:right;">
            <button onclick="closeDuplicateInspectorModal()" class="btn-reset" style="padding:.5rem 1.25rem; font-size:.85rem; border-radius:8px;">Close Inspector</button>
        </div>
    </div>
</div>

<!-- ══════════════════════════════════════════════════════════
     SUCCESS MODAL
══════════════════════════════════════════════════════════ -->
<div id="commit-success-modal" style="display:none; position:fixed; top:0; left:0; width:100vw; height:100vh; background:rgba(15,23,42,.7); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(6px);">
    <div style="background:#ffffff; width:90%; max-width:480px; border-radius:18px; padding:2.2rem 2rem 2rem 2rem; box-shadow:0 25px 60px -12px rgba(0,0,0,.35), 0 0 0 1px rgba(255,255,255,0.2); text-align:center; position:relative; overflow:hidden;">
        <!-- Top decorative color accent line -->
        <div style="position:absolute; top:0; left:0; width:100%; height:5px; background:linear-gradient(90deg, #16a34a, #22c55e, #16a34a);"></div>

        <!-- Success Icon Badge -->
        <div style="width:72px; height:72px; background:#dcfce7; border:2.5px solid #86efac; border-radius:50%; display:inline-flex; align-items:center; justify-content:center; color:#16a34a; font-size:2.2rem; margin-bottom:1.1rem; box-shadow:0 8px 24px rgba(22,163,74,0.22);">
            <i class="fa-solid fa-circle-check"></i>
        </div>

        <h3 style="font-size:1.35rem; font-weight:800; color:#0f172a; margin:0 0 .4rem 0; letter-spacing:-.01em;">Submission Complete!</h3>
        <p id="success-modal-msg" style="font-size:.9rem; color:#475569; margin:0 0 1.35rem 0; line-height:1.45;"></p>

        <!-- Password Vault Card -->
        <div style="background:#f8fafc; border:1.5px solid #e2e8f0; border-radius:14px; padding:1.1rem 1.2rem; margin-bottom:1.5rem; text-align:left;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:.55rem;">
                <span style="font-size:.72rem; font-weight:800; color:#475569; text-transform:uppercase; letter-spacing:.06em; display:flex; align-items:center; gap:.4rem;">
                    <i class="fa-solid fa-lock" style="color:var(--airtel-red);"></i> Sheet Protection Status
                </span>
                <span style="font-size:.68rem; font-weight:700; color:#15803d; background:#dcfce7; padding:2px 8px; border-radius:12px; border:1px solid #bbf7d0; display:inline-flex; align-items:center; gap:.3rem;">
                    <i class="fa-solid fa-shield-halved"></i> Protected &amp; Locked
                </span>
            </div>
            
            <div style="background:#ffffff; border:1.5px solid #cbd5e1; border-radius:9px; padding:.65rem .85rem; display:flex; align-items:center; justify-content:space-between; gap:.75rem; box-shadow:inset 0 1px 2px rgba(0,0,0,0.03);">
                <span style="font-size:.9rem; font-weight:700; color:#334155; display:flex; align-items:center; gap:.5rem;">
                    <i class="fa-solid fa-vault" style="color:var(--airtel-red);"></i> Admin Unlock Password
                </span>
                <span style="font-size:.82rem; font-weight:800; color:#64748b; background:#f1f5f9; padding:4px 10px; border-radius:6px; border:1px solid #e2e8f0; letter-spacing:.12em;">
                    ••••••••••••
                </span>
            </div>
            
            <div style="font-size:.73rem; color:#64748b; margin-top:.6rem; display:flex; align-items:flex-start; gap:.4rem; line-height:1.45;">
                <i class="fa-solid fa-circle-info" style="color:#0284c7; margin-top:2px;"></i>
                <span>The generated Excel file is protected against cell modifications. The unlock key is stored in the <strong>Admin Vault</strong>. Contact your Compliance Administrator if unlocking is required.</span>
            </div>
        </div>

        <!-- Actions Row -->
        <div style="display:flex; gap:.75rem; justify-content:center; align-items:center;">
            <a id="btn-modal-download" href="#" onclick="triggerModalDownloadAndRedirect(); return false;" class="btn-filter" style="padding:.65rem 1.35rem; text-decoration:none; font-size:.875rem; font-weight:700; border-radius:10px; background:var(--airtel-red); color:#fff; display:inline-flex; align-items:center; gap:.5rem; box-shadow:0 4px 14px rgba(220,38,38,0.35); transition:all 0.15s ease;">
                <i class="fa-solid fa-file-arrow-down"></i> Download Locked Copy
            </a>
            <button onclick="closeSuccessModal()" class="btn-reset" style="padding:.65rem 1.2rem; font-size:.875rem; font-weight:700; border-radius:10px; color:#475569; background:#f1f5f9; border:1px solid #cbd5e1; cursor:pointer; transition:all 0.15s ease;" onmouseover="this.style.background='#e2e8f0'" onmouseout="this.style.background='#f1f5f9'">
                Close
            </button>
        </div>
    </div>
</div>

<script>
/* ─── State & Pagination Configuration ────────────────── */
let gridRows               = [];
let isValidating           = false;
let currentDraftId         = <?= $draftId ?>;
let currentDraftName       = '';
let isDraftOwner           = true;
let draftOwnerName         = '';
let lastSyncTs             = 0;
let pollTimer              = null;
let heartbeatTimer         = null;
let activePresence         = [];
let activeViewers          = [];
let latestDuplicateDetails = [];
let currentCardFilter      = 'all';

// Pagination & Virtual Scroll Configuration
let currentPage            = 1;
let pageSize               = 50000; // Default 50,000 rows per page as requested
let totalPages             = 1;
let filteredRowIndices     = [];    // Array of global row indices matching filter
const ROW_HEIGHT           = 38;    // Pixels per table row
const BUFFER_ROWS          = 25;    // Render buffer above and below visible viewport
let scrollTicking          = false;
let expandedDrawers        = new Set();

// Undo / Redo Stacks for Excel Keyboard Shortcuts
let undoStack              = [];
let redoStack              = [];

/* ─── Init ─────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
    setupCellDelegation();
    setupKeyboardShortcuts();
    openDraft(currentDraftId);

    document.addEventListener('click', e => {
        const popover = document.getElementById('officer-inline-popover');
        if (popover && popover.style.display === 'block' && !popover.contains(e.target) && !e.target.closest('.viewer-avatar') && !e.target.closest('.presence-box-badge')) {
            closeOfficerPopover();
        }
    });
});

/* ─── Owner Permissions ────────────────────────────────── */
function applyOwnerPermissions() {
    const btn = document.getElementById('btn-submit-locked');
    if (!btn) return;
    btn.disabled = false;
    btn.style.opacity = '1';
    btn.style.cursor = 'pointer';
    btn.style.background = '#16a34a';
    btn.style.color = '#fff';
    btn.innerHTML = '<i class="fa-solid fa-cloud-arrow-up"></i> Upload &amp; Lock File';
    btn.title = '';
}

/* ─── Open existing draft ──────────────────────────────── */
async function openDraft(draftId) {
    const statusInd = document.getElementById('grid-status');
    if (statusInd) statusInd.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Loading file data…';

    try {
        const res  = await fetch(`ajax/sandbox_draft_api.php?action=get&draft_id=${draftId}`);
        const data = await res.json();
        if (data.status !== 'success') throw new Error(data.message || 'Error loading draft file');

        gridRows         = cleanRawRows(data.rows || []);
        currentDraftId   = data.draft_id;
        currentDraftName = data.draft_name;
        isDraftOwner     = (data.is_owner !== undefined) ? data.is_owner : true;
        draftOwnerName   = data.created_by || '';
        lastSyncTs       = data.updated_ts || 0;
        activePresence   = data.presence   || [];
        activeViewers    = data.viewers    || [];
        
        undoStack        = [];
        redoStack        = [];
        currentCardFilter= 'all';
        currentPage      = 1;
        expandedDrawers.clear();

        document.getElementById('workspace-filename').textContent = currentDraftName || 'Draft';

        const bannerWrap = document.getElementById('flagged-admin-banner-wrap');
        if (bannerWrap) {
            if (data.draft_status === 'flagged') {
                bannerWrap.innerHTML = `
                <div style="background:#fffbeb; border:1.5px solid #fcd34d; border-radius:12px; padding:.95rem 1.25rem; margin-bottom:1.1rem; display:flex; align-items:center; justify-content:space-between; gap:1rem; flex-wrap:wrap; box-shadow:0 4px 14px rgba(217,119,6,0.12);">
                    <div style="display:flex; align-items:center; gap:.85rem;">
                        <div style="width:40px; height:40px; background:#fef3c7; border-radius:50%; display:flex; align-items:center; justify-content:center; color:#d97706; font-size:1.2rem; flex-shrink:0;">
                            <i class="fa-solid fa-triangle-exclamation"></i>
                        </div>
                        <div>
                            <div style="font-weight:800; font-size:.95rem; color:#92400e;">⚠️ Admin Flagged Duplicate Review Notice</div>
                            <div style="font-size:.85rem; color:#b45309; margin-top:2px;">${escHtml(data.flagged_reason || 'An administrator flagged potential duplicate claims in this report. Edit cells directly in the grid below to resolve.')}</div>
                        </div>
                    </div>
                    <div style="font-size:.78rem; font-weight:700; color:#b45309; background:#fef3c7; padding:.35rem .85rem; border-radius:20px; border:1px solid #fcd34d;">
                        <i class="fa-solid fa-pen-to-square"></i> Direct Web-Grid Edit Active
                    </div>
                </div>`;
            } else {
                bannerWrap.innerHTML = '';
            }
        }

        applyOwnerPermissions();
        renderGridTable();
        validateGrid();
        startHeartbeat();
        startLiveSync();
    } catch (err) {
        alert('Could not open file: ' + err.message);
        window.location.href = 'data_preparation.php';
    }
}

/* ─── Card Filter Trigger ──────────────────────────────── */
function setCardFilter(filterType) {
    currentCardFilter = filterType;
    currentPage       = 1; // Reset to page 1 on filter switch

    ['card-total','card-clean','card-critical','card-warnings','card-duplicates'].forEach(id => {
        const card = document.getElementById(id);
        if (card) card.style.borderColor = '#e2e8f0';
    });

    const activeCard = document.getElementById(`card-${filterType}`);
    if (activeCard) activeCard.style.borderColor = '#1d4ed8';

    const ind = document.getElementById('active-filter-indicator');
    if (ind) {
        const labels = {
            all: 'Showing All Rows',
            clean: 'Filtered: Clean Rows Only',
            critical: 'Filtered: Critical Errors Only',
            warnings: 'Filtered: Warnings Only',
            duplicates: 'Filtered: Duplicate Conflicts Only'
        };
        ind.textContent = labels[filterType] || 'Showing All Rows';
    }

    // Update Export Filter button label to reflect current filter
    const exportBtn = document.getElementById('btn-export-filter');
    if (exportBtn) {
        const exportLabels = {
            all:        'Export All',
            clean:      'Export Clean',
            critical:   'Export Errors',
            warnings:   'Export Warnings',
            duplicates: 'Export Dups'
        };
        exportBtn.innerHTML = `<i class="fa-solid fa-filter"></i> ${exportLabels[filterType] || 'Export Filter'}`;
    }

    const tableWrap = document.getElementById('table-scroll-wrap');
    if (tableWrap) tableWrap.scrollTop = 0;

    renderGridTable();
}

/* ─── Pagination Controls ──────────────────────────────── */
function changePageSize(newSize) {
    pageSize = parseInt(newSize) || 50000;
    currentPage = 1;
    const tableWrap = document.getElementById('table-scroll-wrap');
    if (tableWrap) tableWrap.scrollTop = 0;
    renderGridTable();
}

function goToPage(page) {
    page = Math.max(1, Math.min(totalPages, parseInt(page) || 1));
    if (page === currentPage) return;
    currentPage = page;
    const tableWrap = document.getElementById('table-scroll-wrap');
    if (tableWrap) tableWrap.scrollTop = 0;
    renderVirtualRows(true);
    updatePaginationUI();
}

function updatePaginationUI() {
    const totalFiltered = filteredRowIndices.length;
    totalPages = Math.max(1, Math.ceil(totalFiltered / pageSize));
    if (currentPage > totalPages) currentPage = totalPages;

    const startRow = totalFiltered === 0 ? 0 : (currentPage - 1) * pageSize + 1;
    const endRow   = Math.min(totalFiltered, currentPage * pageSize);

    const summaryText = `Showing rows ${startRow.toLocaleString()} – ${endRow.toLocaleString()} of ${totalFiltered.toLocaleString()} (Page ${currentPage} of ${totalPages})`;
    
    const topSummary = document.getElementById('pagination-top-summary');
    if (topSummary) topSummary.textContent = summaryText;

    const bottomSummary = document.getElementById('pagination-bottom-summary');
    if (bottomSummary) bottomSummary.textContent = `Showing ${startRow.toLocaleString()} – ${endRow.toLocaleString()} of ${totalFiltered.toLocaleString()} rows`;

    const bottomBadge = document.getElementById('pagination-bottom-page-badge');
    if (bottomBadge) bottomBadge.textContent = `Page ${currentPage} / ${totalPages}`;

    const totalLabel = document.getElementById('page-total-label');
    if (totalLabel) totalLabel.textContent = `of ${totalPages}`;

    // Update Dropdown options
    const pageSelect = document.getElementById('select-current-page');
    if (pageSelect) {
        let optionsHtml = '';
        for (let p = 1; p <= totalPages; p++) {
            const pStart = (p - 1) * pageSize + 1;
            const pEnd   = Math.min(totalFiltered, p * pageSize);
            optionsHtml += `<option value="${p}" ${p === currentPage ? 'selected' : ''}>Page ${p} (${pStart.toLocaleString()} – ${pEnd.toLocaleString()})</option>`;
        }
        pageSelect.innerHTML = optionsHtml;
    }

    // Update First/Prev/Next/Last disabled state
    const isFirst = (currentPage <= 1);
    const isLast  = (currentPage >= totalPages);

    const btnFirst = document.getElementById('btn-page-first');
    const btnPrev  = document.getElementById('btn-page-prev');
    const btnNext  = document.getElementById('btn-page-next');
    const btnLast  = document.getElementById('btn-page-last');

    [btnFirst, btnPrev].forEach(b => {
        if (b) {
            b.disabled = isFirst;
            b.style.opacity = isFirst ? '0.45' : '1';
            b.style.cursor = isFirst ? 'not-allowed' : 'pointer';
        }
    });

    [btnNext, btnLast].forEach(b => {
        if (b) {
            b.disabled = isLast;
            b.style.opacity = isLast ? '0.45' : '1';
            b.style.cursor = isLast ? 'not-allowed' : 'pointer';
        }
    });
}

/* ─── Cell Delegation & Excel Keyboard Shortcuts ──────────── */
function setupCellDelegation() {
    document.getElementById('spreadsheet-table').addEventListener('click', e => {
        const td = e.target.closest('td[contenteditable="true"]');
        if (!td) return;
        const row   = parseInt(td.dataset.row);
        const field = td.dataset.field;
        if (!isNaN(row) && field) broadcastCellFocus(row, field);
    });

    document.getElementById('spreadsheet-table').addEventListener('keydown', e => {
        const td = e.target.closest('td[contenteditable="true"]');
        if (!td) return;

        const row = parseInt(td.dataset.row);
        const field = td.dataset.field;
        const fields = ['owner_name', 'id_passport_no', 'date_of_birth', 'account_number', 'last_transaction', 'due_amount', 'compilation_date'];
        const fieldIdx = fields.indexOf(field);

        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            td.blur();
            const nextTd = document.querySelector(`td[data-row="${row + 1}"][data-field="${field}"]`);
            if (nextTd) { nextTd.focus(); }
        } else if (e.key === 'Tab') {
            e.preventDefault();
            td.blur();
            let nextFieldIdx = e.shiftKey ? fieldIdx - 1 : fieldIdx + 1;
            let nextRow = row;
            if (nextFieldIdx >= fields.length) { nextFieldIdx = 0; nextRow++; }
            else if (nextFieldIdx < 0) { nextFieldIdx = fields.length - 1; nextRow--; }

            const nextTd = document.querySelector(`td[data-row="${nextRow}"][data-field="${fields[nextFieldIdx]}"]`);
            if (nextTd) { nextTd.focus(); }
        }
    });
}

function setupKeyboardShortcuts() {
    document.addEventListener('keydown', e => {
        const workspace = document.getElementById('grid-workspace');
        if (!workspace || workspace.style.display === 'none') return;

        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z' && !e.shiftKey) {
            e.preventDefault();
            triggerUndo();
        }
        else if (((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'y') || ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key.toLowerCase() === 'z')) {
            e.preventDefault();
            triggerRedo();
        }
    });
}

function pushStateToHistory() {
    undoStack.push(JSON.parse(JSON.stringify(gridRows)));
    if (undoStack.length > 20) undoStack.shift();
    redoStack = [];
}

function triggerUndo() {
    if (!undoStack.length) {
        showStatusNotice('Nothing to undo');
        return;
    }
    redoStack.push(JSON.parse(JSON.stringify(gridRows)));
    gridRows = undoStack.pop();
    validateGrid();
    saveInitialDraft();
    showStatusNotice('↺ Undone');
}

function triggerRedo() {
    if (!redoStack.length) {
        showStatusNotice('Nothing to redo');
        return;
    }
    undoStack.push(JSON.parse(JSON.stringify(gridRows)));
    gridRows = redoStack.pop();
    validateGrid();
    saveInitialDraft();
    showStatusNotice('↻ Redone');
}

function showStatusNotice(msg) {
    const ind = document.getElementById('grid-status');
    if (ind) {
        ind.textContent = msg;
        setTimeout(() => { if (ind.textContent === msg) ind.textContent = 'Live Validation Active'; }, 2000);
    }
}

/* ─── Presence broadcast (others only) ─────────────────── */
async function broadcastCellFocus(rowIdx, field) {
    if (!currentDraftId) return;
    try {
        const res  = await fetch('ajax/sandbox_draft_api.php?action=update_presence', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ draft_id: currentDraftId, row_index: rowIdx, field })
        });
        const data = await res.json();
        if (data.status === 'success') {
            activePresence = data.presence || [];
            renderPresenceOverlay(activePresence);
        }
    } catch (e) {}
}

/* ─── Floating Presence Overlay ───────────────────────── */
function renderPresenceOverlay(arr) {
    const overlay   = document.getElementById('presence-overlay-layer');
    const tableWrap = document.getElementById('table-scroll-wrap');
    if (!overlay || !tableWrap) return;
    overlay.innerHTML = '';
    if (!arr || !arr.length) return;

    arr.sort((a, b) => String(a.user).localeCompare(String(b.user)));

    const wrapRect = tableWrap.getBoundingClientRect();
    arr.forEach(p => {
        const cell = document.querySelector(`td[data-row="${p.row}"][data-field="${p.field}"]`);
        if (!cell) return;
        const cr = cell.getBoundingClientRect();

        const box = document.createElement('div');
        box.className     = 'presence-box';
        box.style.top     = (cr.top  - wrapRect.top  + tableWrap.scrollTop)  + 'px';
        box.style.left    = (cr.left - wrapRect.left + tableWrap.scrollLeft) + 'px';
        box.style.width   = cr.width  + 'px';
        box.style.height  = cr.height + 'px';
        box.style.borderColor = p.color;

        const badge = document.createElement('div');
        badge.className        = 'presence-box-badge';
        badge.style.background = p.color;
        badge.title            = `${p.user} — editing Row #${p.row + 1}`;
        badge.textContent      = p.initials || '??';
        badge.onclick          = (evt) => {
            evt.stopPropagation();
            openOfficerPopover(evt, p.user, p.color, p.row, p.field);
        };

        box.appendChild(badge);
        overlay.appendChild(box);
    });
}
function repositionOverlay() { renderPresenceOverlay(activePresence); }

/* ─── Viewer Avatar Bar (Alphabetical Order + Target Popover) ── */
function renderViewerBar(viewers) {
    const bar = document.getElementById('viewer-avatar-stack');
    if (!bar) return;
    bar.innerHTML = '';

    const sortedViewers = (viewers || []).slice();
    sortedViewers.sort((a, b) => String(a.user).localeCompare(String(b.user)));

    sortedViewers.forEach(v => {
        const a = document.createElement('div');
        a.className        = 'viewer-avatar';
        a.style.background = v.color;
        a.title            = v.user + (v.is_self ? ' (You)' : '');
        a.textContent      = v.initials || '?';

        const editingInfo = activePresence.find(p => p.user === v.user);
        a.onclick = (evt) => {
            evt.stopPropagation();
            if (editingInfo) {
                openOfficerPopover(evt, v.user, v.color, editingInfo.row, editingInfo.field);
            } else {
                openOfficerPopover(evt, v.user, v.color, -1, '');
            }
        };

        bar.appendChild(a);
    });
}

/* ─── Sleek Target-Anchored Popover & Go To Location ──── */
function openOfficerPopover(evt, userName, color, rowIdx, field) {
    const popover   = document.getElementById('officer-inline-popover');
    const avatar    = document.getElementById('popover-avatar');
    const nameEl    = document.getElementById('popover-name');
    const statusEl  = document.getElementById('popover-status');
    const actionWrap= document.getElementById('popover-action-wrap');

    avatar.style.background = color || '#1d4ed8';
    avatar.textContent      = getUserInitials(userName);
    nameEl.textContent      = userName;

    const fieldNames = {
        owner_name: 'Owner Name', id_passport_no: 'ID / Passport No',
        date_of_birth: 'Date of Birth', account_number: 'Account Number',
        last_transaction: 'Last Transaction', due_amount: 'Due Amount', compilation_date: 'Compilation Date'
    };

    if (rowIdx >= 0 && field) {
        statusEl.innerHTML = `<span style="color:#15803d; font-weight:700;"><i class="fa-solid fa-pen-to-square"></i> Editing Row #${rowIdx + 1}</span><br><span style="font-size:0.7rem; color:#64748b;">(${fieldNames[field] || field})</span>`;
        actionWrap.innerHTML = `
            <button onclick="jumpToCell(${rowIdx}, '${field}')" class="btn-filter" style="width:100%; padding:.45rem .75rem; border-radius:7px; font-size:.8rem; display:inline-flex; align-items:center; justify-content:center; gap:.4rem;">
                <i class="fa-solid fa-crosshairs"></i> Go to Location
            </button>`;
    } else {
        statusEl.innerHTML = `<span style="color:#0369a1; font-weight:600;"><i class="fa-solid fa-eye"></i> Currently viewing file</span>`;
        actionWrap.innerHTML = '';
    }

    popover.style.display = 'block';

    if (evt && evt.target) {
        const targetRect = evt.target.getBoundingClientRect();
        let top = targetRect.bottom + window.scrollY + 6;
        let left = targetRect.left + window.scrollX - 90;

        if (left < 10) left = 10;
        if (left + 260 > window.innerWidth) left = window.innerWidth - 270;

        popover.style.top = top + 'px';
        popover.style.left = left + 'px';
    }
}

function closeOfficerPopover() {
    const popover = document.getElementById('officer-inline-popover');
    if (popover) popover.style.display = 'none';
}

function jumpToCell(rowIdx, field) {
    closeOfficerPopover();

    // Determine if this row is currently filtered out
    const filterItemIdx = filteredRowIndices.indexOf(rowIdx);
    if (filterItemIdx === -1) {
        setCardFilter('all');
    }

    // Calculate target page
    const newFilterIdx = filteredRowIndices.indexOf(rowIdx);
    if (newFilterIdx !== -1) {
        const targetPage = Math.floor(newFilterIdx / pageSize) + 1;
        if (currentPage !== targetPage) {
            goToPage(targetPage);
        }

        // Scroll within the page
        const rowInPage = newFilterIdx % pageSize;
        const tableWrap = document.getElementById('table-scroll-wrap');
        if (tableWrap) {
            tableWrap.scrollTop = Math.max(0, (rowInPage * ROW_HEIGHT) - 160);
            renderVirtualRows(true);

            setTimeout(() => {
                const cell = document.querySelector(`td[data-row="${rowIdx}"][data-field="${field}"]`);
                if (cell) {
                    cell.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'nearest' });
                    cell.classList.remove('jump-highlight');
                    void cell.offsetWidth;
                    cell.classList.add('jump-highlight');
                    setTimeout(() => { cell.classList.remove('jump-highlight'); }, 2500);
                }
            }, 100);
        }
    }
}

function getUserInitials(name) {
    const parts = String(name || '').trim().split(' ');
    if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
    return String(name || 'U').substring(0, 2).toUpperCase();
}

/* ─── Duplicate Inspector Modal & Logic ───────────────────── */
function openDuplicateInspectorModal() {
    const modal   = document.getElementById('duplicate-inspector-modal');
    const content = document.getElementById('duplicate-modal-content');
    if (!modal || !content) return;

    if (!latestDuplicateDetails || latestDuplicateDetails.length === 0) {
        content.innerHTML = `
            <div style="text-align:center; padding:3rem 1rem; color:#64748b;">
                <i class="fa-solid fa-circle-check" style="font-size:2.5rem; color:#16a34a; margin-bottom:.75rem; display:block;"></i>
                <div style="font-weight:700; font-size:1.05rem; color:var(--text-primary); margin-bottom:.3rem;">No Duplicate Conflicts Detected</div>
                <div style="font-size:.85rem;">All records in this sheet are unique and clear of historical database matches.</div>
            </div>`;
    } else {
        const inFileDups = latestDuplicateDetails.filter(d => d.type === 'in_file');
        const dbDups     = latestDuplicateDetails.filter(d => d.type === 'database');
        const thStyle    = 'padding:.5rem .65rem; border:1px solid #c7d2fe; background:#eef2ff; color:#3730a3; font-size:.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.04em; white-space:nowrap;';
        const thStyleRed = 'padding:.5rem .65rem; border:1px solid #fca5a5; background:#fef2f2; color:#7f1d1d; font-size:.68rem; font-weight:700; text-transform:uppercase; letter-spacing:.04em; white-space:nowrap;';
        let html = '';

        /* ── In-File Duplicates Table ── */
        if (inFileDups.length > 0) {
            html += `
            <div style="margin-bottom:2rem;">
                <div style="font-size:.85rem; font-weight:800; color:#4338ca; background:#e0e7ff; padding:.5rem .9rem; border-radius:8px; margin-bottom:.8rem; display:flex; align-items:center; justify-content:space-between;">
                    <span><i class="fa-solid fa-copy"></i>&nbsp; In-File Sheet Conflicts &nbsp;<span style="background:#4338ca;color:#fff;border-radius:99px;padding:1px 9px;">${inFileDups.length}</span></span>
                    <span style="font-size:.72rem; font-weight:600; opacity:.8;">All 6 key fields are identical within this uploaded sheet</span>
                </div>
                <div style="overflow-x:auto; overflow-y:auto; border-radius:9px; border:1.5px solid #c7d2fe; max-height:350px; overscroll-behavior:contain; will-change:scroll-position;">
                    <table style="width:100%; border-collapse:collapse; font-size:.8rem;">
                        <thead style="position:sticky; top:0; z-index:5;">
                            <tr>
                                <th style="${thStyle} text-align:center; width:50px;">Row #</th>
                                <th style="${thStyle} text-align:center; width:80px;">Matches</th>
                                <th style="${thStyle}">Owner Name</th>
                                <th style="${thStyle}">ID / Passport No</th>
                                <th style="${thStyle}">Date of Birth</th>
                                <th style="${thStyle}">Account Number</th>
                                <th style="${thStyle}">Last Transaction</th>
                                <th style="${thStyle} text-align:right;">Due Amount (KES)</th>
                                <th style="${thStyle}">Compilation Date</th>
                                <th style="${thStyle} text-align:center; width:55px;">Go To</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${inFileDups.map((d, i) => `
                            <tr style="background:${i%2===0?'#fff':'#f5f3ff'}; border-left:3px solid #6366f1;">
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; text-align:center; font-weight:800; color:#4f46e5;">#${d.row_num}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; text-align:center; color:#7c3aed; font-weight:700; font-size:.73rem;">↔ #${d.matched_with_row}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; font-weight:600; white-space:nowrap;">${escHtml(d.owner_name)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; white-space:nowrap;">${escHtml(d.id_passport_no)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; white-space:nowrap;">${escHtml(d.date_of_birth)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; font-weight:700; white-space:nowrap;">${escHtml(d.account_number)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; white-space:nowrap;">${escHtml(d.last_transaction)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; text-align:right; font-weight:700; white-space:nowrap;">${escHtml(d.due_amount)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; white-space:nowrap;">${escHtml(d.compilation_date)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #e0e7ff; text-align:center;">
                                    <button onclick="closeDuplicateInspectorModal(); jumpToCell(${d.row_num-1},'account_number');"
                                        class="btn-filter" style="padding:.26rem .55rem; font-size:.72rem; border-radius:6px;"
                                        title="Jump to row #${d.row_num} in the grid">
                                        <i class="fa-solid fa-crosshairs"></i>
                                    </button>
                                </td>
                            </tr>`).join('')}
                        </tbody>
                    </table>
                </div>
            </div>`;
        }

        /* ── Historical DB Duplicates Table ── */
        if (dbDups.length > 0) {
            html += `
            <div>
                <div style="font-size:.85rem; font-weight:800; color:#991b1b; background:#fee2e2; padding:.5rem .9rem; border-radius:8px; margin-bottom:.8rem; display:flex; align-items:center; justify-content:space-between;">
                    <span><i class="fa-solid fa-database"></i>&nbsp; Historical Database Matches &nbsp;<span style="background:#dc2626;color:#fff;border-radius:99px;padding:1px 9px;">${dbDups.length}</span></span>
                    <span style="font-size:.72rem; font-weight:600; opacity:.8;">All 6 key fields match historical records in the MySQL database</span>
                </div>
                <div style="overflow-x:auto; overflow-y:auto; border-radius:9px; border:1.5px solid #fca5a5; max-height:350px; overscroll-behavior:contain; will-change:scroll-position;">
                    <table style="width:100%; border-collapse:collapse; font-size:.8rem;">
                        <thead style="position:sticky; top:0; z-index:5;">
                            <tr>
                                <th style="${thStyleRed} text-align:center; width:60px;">Sheet Row</th>
                                <th style="${thStyleRed} text-align:center; width:65px;">DB Record</th>
                                <th style="${thStyleRed}">Owner Name</th>
                                <th style="${thStyleRed}">ID / Passport No</th>
                                <th style="${thStyleRed}">Date of Birth</th>
                                <th style="${thStyleRed}">Account Number</th>
                                <th style="${thStyleRed}">Last Transaction</th>
                                <th style="${thStyleRed} text-align:right;">Due Amount (KES)</th>
                                <th style="${thStyleRed}">Compilation Date</th>
                                <th style="${thStyleRed}">DB Uploaded On</th>
                                <th style="${thStyleRed} text-align:center; width:55px;">Go To</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${dbDups.map((d, i) => `
                            <tr style="background:${i%2===0?'#fff5f5':'#fff'}; border-left:3px solid #dc2626;">
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; text-align:center; font-weight:800; color:#dc2626;">#${d.row_num}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; text-align:center;">
                                    <span style="background:#dc2626;color:#fff;font-size:.7rem;font-weight:800;padding:2px 7px;border-radius:5px;">DB&nbsp;#${d.db_id}</span>
                                </td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; font-weight:600; white-space:nowrap;">${escHtml(d.owner_name || d.db_owner_name)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; white-space:nowrap;">${escHtml(d.id_passport_no)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; white-space:nowrap;">${escHtml(d.date_of_birth)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; font-weight:700; white-space:nowrap;">${escHtml(d.account_number)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; white-space:nowrap;">${escHtml(d.last_transaction)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; text-align:right; font-weight:700; white-space:nowrap;">${escHtml(d.due_amount)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; white-space:nowrap;">${escHtml(d.compilation_date)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; font-size:.72rem; color:#64748b; white-space:nowrap;">${escHtml(d.created_at)}</td>
                                <td style="padding:.48rem .65rem; border:1px solid #fecaca; text-align:center;">
                                    <button onclick="closeDuplicateInspectorModal(); jumpToCell(${d.row_num-1},'account_number');"
                                        class="btn-reset" style="padding:.26rem .55rem; font-size:.72rem; border-radius:6px; color:#dc2626; border:1px solid #fca5a5;"
                                        title="Jump to row #${d.row_num} in the grid">
                                        <i class="fa-solid fa-crosshairs"></i>
                                    </button>
                                </td>
                            </tr>`).join('')}
                        </tbody>
                    </table>
                </div>
            </div>`;
        }

        content.innerHTML = html;
    }

    modal.style.display = 'flex';
}

function closeDuplicateInspectorModal() {
    const modal = document.getElementById('duplicate-inspector-modal');
    if (modal) modal.style.display = 'none';
}

/* ─── Heartbeat ────────────────────────────────────────── */
function startHeartbeat() {
    clearInterval(heartbeatTimer);
    sendHeartbeat();
    heartbeatTimer = setInterval(sendHeartbeat, 8000);
}
async function sendHeartbeat() {
    if (!currentDraftId) return;
    try {
        const res  = await fetch('ajax/sandbox_draft_api.php?action=heartbeat', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ draft_id: currentDraftId })
        });
        const data = await res.json();
        if (data.status === 'success') { activeViewers = data.viewers || []; renderViewerBar(activeViewers); }
    } catch (e) {}
}

/* ─── Lightweight Live Sync (Does NOT transfer megabytes) ─ */
function startLiveSync() {
    clearInterval(pollTimer);
    pollTimer = setInterval(async () => {
        if (!currentDraftId || isValidating) return;
        try {
            const res  = await fetch(`ajax/sandbox_draft_api.php?action=poll_sync&draft_id=${currentDraftId}`);
            const data = await res.json();

            // Real-Time Auto Close when file is submitted & locked by another officer
            if (data.status === 'closed' || (data.status === 'error' && data.message && data.message.includes('not found'))) {
                clearInterval(pollTimer);
                clearInterval(heartbeatTimer);
                showDraftClosedModal(data.submitted_by || 'another officer');
                return;
            }

            if (data.status !== 'success') return;

            activePresence = data.presence || [];
            renderPresenceOverlay(activePresence);
            renderViewerBar(data.viewers || activeViewers);

            // Only fetch full rows if another user saved changes
            if (data.updated_ts && data.updated_ts !== lastSyncTs) {
                lastSyncTs = data.updated_ts;
                if (data.last_edited_by && data.last_edited_by !== data.current_user) {
                    const fullRes  = await fetch(`ajax/sandbox_draft_api.php?action=get&draft_id=${currentDraftId}`);
                    const fullData = await fullRes.json();
                    if (fullData.status === 'success') {
                        gridRows = cleanRawRows(fullData.rows || []);
                        validateGrid();
                        document.getElementById('workspace-meta').textContent = `Live Editing · Last update by ${data.last_edited_by}`;
                    }
                }
            }
        } catch (e) {}
    }, 3500);
}

let isClosedModalActive = false;

function showDraftClosedModal(submittedBy) {
    if (isClosedModalActive) return;
    isClosedModalActive = true;

    clearInterval(pollTimer);
    clearInterval(heartbeatTimer);

    const successModal = document.getElementById('commit-success-modal');
    if (successModal && successModal.style.display === 'flex') return;

    let closedModal = document.getElementById('draft-closed-modal');
    if (!closedModal) {
        closedModal = document.createElement('div');
        closedModal.id = 'draft-closed-modal';
        closedModal.style.cssText = 'position:fixed; top:0; left:0; width:100vw; height:100vh; background:rgba(15,23,42,.75); z-index:10000; display:flex; align-items:center; justify-content:center; backdrop-filter:blur(6px);';
        closedModal.innerHTML = `
            <div style="background:#fff; width:90%; max-width:440px; border-radius:18px; padding:2rem; text-align:center; box-shadow:0 25px 60px -12px rgba(0,0,0,.35);">
                <div style="width:68px; height:68px; background:#fef2f2; border:2px solid #fca5a5; border-radius:50%; display:inline-flex; align-items:center; justify-content:center; color:#dc2626; font-size:2rem; margin-bottom:1.1rem; box-shadow:0 8px 24px rgba(220,38,38,0.2);">
                    <i class="fa-solid fa-lock"></i>
                </div>
                <h3 style="font-size:1.3rem; font-weight:800; color:#0f172a; margin:0 0 .45rem 0;">File Submitted &amp; Locked</h3>
                <p style="font-size:.9rem; color:#475569; margin:0 0 1.25rem 0; line-height:1.5;">
                    This working file has just been submitted and locked by owner <strong>${escHtml(submittedBy)}</strong>. This workspace is now closed for active editors.
                </p>
                <div style="display:flex; flex-direction:column; gap:.85rem; align-items:center;">
                    <button onclick="window.location.replace('data_preparation.php')" class="btn-filter" style="padding:.65rem 1.35rem; font-size:.875rem; font-weight:700; border-radius:10px; background:var(--airtel-red); color:#fff; cursor:pointer; display:inline-flex; align-items:center; gap:.5rem; box-shadow:0 4px 14px rgba(220,38,38,0.35);">
                        <i class="fa-solid fa-arrow-left"></i> Return to Working Files Now
                    </button>
                    <div style="font-size:.82rem; font-weight:700; color:#64748b; background:#f1f5f9; padding:.4rem .9rem; border-radius:20px; border:1px solid #cbd5e1; display:inline-flex; align-items:center; gap:.4rem;">
                        <i class="fa-solid fa-clock" style="color:var(--airtel-red);"></i> Auto-redirecting in <span id="closed-countdown-sec" style="color:var(--airtel-red); font-weight:800; font-size:.95rem;">5</span>s
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(closedModal);
    } else {
        closedModal.style.display = 'flex';
    }

    let remaining = 5;
    const secSpan = document.getElementById('closed-countdown-sec');
    if (secSpan) secSpan.textContent = remaining;

    const timer = setInterval(() => {
        remaining--;
        if (secSpan) secSpan.textContent = Math.max(0, remaining);
        if (remaining <= 0) {
            clearInterval(timer);
            const badgeWrap = secSpan ? secSpan.parentElement : null;
            if (badgeWrap) {
                badgeWrap.innerHTML = '<i class="fa-solid fa-spinner fa-spin" style="color:var(--airtel-red);"></i> Redirecting now…';
            }
            window.location.replace('data_preparation.php');
        }
    }, 1000);
}

/* ─── High-Performance Server Validation ──────────────────── */
async function validateGrid() {
    if (isValidating) return;
    isValidating = true;
    const ind = document.getElementById('grid-status');
    if (ind) ind.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Checking duplicates in DB…';

    try {
        const payload = { draft_id: currentDraftId };
        if (gridRows.length <= 1500) {
            payload.rows = gridRows;
        }

        const res = await fetch('ajax/pre_submission_validator.php', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const data = await res.json();
        if (data.status !== 'success') throw new Error(data.message || 'Validation error');

        latestDuplicateDetails = data.duplicate_details || [];

        // Merge compact row_issues into gridRows in memory
        const issues = data.row_issues || {};
        for (let i = 0; i < gridRows.length; i++) {
            const iss = issues[i];
            if (iss) {
                gridRows[i].errors       = iss.errors || {};
                gridRows[i].warnings     = iss.warnings || {};
                gridRows[i].has_critical = Object.keys(gridRows[i].errors).length > 0;
                gridRows[i].is_clean     = !gridRows[i].has_critical && Object.keys(gridRows[i].warnings).length === 0;
            } else {
                gridRows[i].errors       = {};
                gridRows[i].warnings     = {};
                gridRows[i].has_critical = false;
                gridRows[i].is_clean     = true;
            }
        }

        updateSummaryCards(data.summary);
        renderGridTable();
        renderPresenceOverlay(activePresence);
    } catch (err) {
        console.error('validateGrid error:', err);
    } finally {
        isValidating = false;
        if (ind) ind.textContent = 'Live Validation Active';
    }
}

/* ─── Grid Filter Computation ────────────────────────────── */
function computeFilteredIndices() {
    filteredRowIndices = [];
    const total = gridRows.length;

    for (let i = 0; i < total; i++) {
        const r = gridRows[i];
        if (currentCardFilter === 'clean') {
            if (r.is_clean) filteredRowIndices.push(i);
        } else if (currentCardFilter === 'critical') {
            if (r.has_critical) filteredRowIndices.push(i);
        } else if (currentCardFilter === 'warnings') {
            if (r.warnings && Object.keys(r.warnings).length > 0) filteredRowIndices.push(i);
        } else if (currentCardFilter === 'duplicates') {
            const rowNum = i + 1;
            const hasDupErr = r.errors && r.errors.account_number && r.errors.account_number.toLowerCase().includes('duplicate');
            const inDetails = latestDuplicateDetails.some(d => d.row_num === rowNum || (d.type === 'in_file' && d.matched_with_row === rowNum));
            if (hasDupErr || inDetails) filteredRowIndices.push(i);
        } else {
            filteredRowIndices.push(i);
        }
    }
}

/* ─── Grid Render Coordinator ───────────────────────────── */
function renderGridTable() {
    computeFilteredIndices();
    updatePaginationUI();
    renderVirtualRows(true);
}

function handleTableScroll() {
    repositionOverlay();
    if (!scrollTicking) {
        requestAnimationFrame(() => {
            renderVirtualRows(false);
            scrollTicking = false;
        });
        scrollTicking = true;
    }
}

/* ─── High-Performance Virtual Window Rendering ─────────── */
function renderVirtualRows(forceReset = false) {
    const tbody = document.getElementById('spreadsheet-tbody');
    const tableWrap = document.getElementById('table-scroll-wrap');
    if (!tbody || !tableWrap) return;

    const totalFiltered = filteredRowIndices.length;
    if (totalFiltered === 0) {
        tbody.innerHTML = `<tr><td colspan="9" style="text-align:center;padding:2.5rem;color:#64748b;font-weight:600;">No records found matching filter "${currentCardFilter}".</td></tr>`;
        return;
    }

    // Get the slice of rows assigned to the current page
    const pageStartIndex = (currentPage - 1) * pageSize;
    const pageEndIndex   = Math.min(totalFiltered, pageStartIndex + pageSize);
    const pageRowCount   = pageEndIndex - pageStartIndex;

    const scrollTop      = tableWrap.scrollTop;
    const viewportHeight = tableWrap.clientHeight || 560;

    const visibleCount   = Math.ceil(viewportHeight / ROW_HEIGHT) + (BUFFER_ROWS * 2);
    const startInPage    = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER_ROWS);
    const endInPage      = Math.min(pageRowCount, startInPage + visibleCount);

    const topSpacerHeight    = startInPage * ROW_HEIGHT;
    const bottomSpacerHeight = Math.max(0, (pageRowCount - endInPage) * ROW_HEIGHT);

    let html = '';

    // Top virtual spacer
    if (topSpacerHeight > 0) {
        html += `<tr style="height:${topSpacerHeight}px;"><td colspan="9" style="padding:0;border:none;background:transparent;"></td></tr>`;
    }

    // Render viewport visible rows
    for (let pIdx = startInPage; pIdx < endInPage; pIdx++) {
        const globalRowIdx = filteredRowIndices[pageStartIndex + pIdx];
        const r = gridRows[globalRowIdx];
        if (!r) continue;

        const rowNum = globalRowIdx + 1;
        const err  = r.errors   || {};
        const warn = r.warnings || {};

        const cst  = f => err[f]  ? 'background:#fef2f2; color:#991b1b; font-weight:700; border:1.5px solid #ef4444;' :
                          warn[f] ? 'background:#fffbeb; color:#92400e; border:1.5px solid #f59e0b;' : '';
        const ct   = f => err[f]  ? `🚫 Error: ${err[f]}` :
                          warn[f] ? `⚠️ Warning: ${warn[f]}` : 'Click to edit cell';

        const cellVal = (f, val) => {
            if (val !== undefined && val !== null && String(val).trim() !== '') {
                return escHtml(val);
            }
            if (err[f] || warn[f]) {
                return `<span style="font-style:italic; opacity:.65;">(missing)</span>`;
            }
            return '';
        };

        const dupDetail = latestDuplicateDetails.find(d => d.row_num === rowNum || (d.type === 'in_file' && d.matched_with_row === rowNum));
        const isDup = !!dupDetail || (err.account_number && err.account_number.toLowerCase().includes('duplicate'));
        const isDrawerOpen = expandedDrawers.has(globalRowIdx);

        let drawerHtml = '';
        if (isDup && isDrawerOpen) {
            const isDb = dupDetail && dupDetail.type === 'database';
            const matchedLabel = isDb 
                ? `<span style="background:#dc2626; color:#fff; font-size:.68rem; font-weight:800; padding:2px 7px; border-radius:5px;"><i class="fa-solid fa-database"></i> Original MySQL DB Record #${dupDetail.db_id}</span>`
                : `<span style="background:#4338ca; color:#fff; font-size:.68rem; font-weight:800; padding:2px 7px; border-radius:5px;"><i class="fa-solid fa-copy"></i> Matched In-File Row #${dupDetail ? (dupDetail.matched_with_row === rowNum ? dupDetail.row_num : dupDetail.matched_with_row) : 'Original'}</span>`;

            const dbOwner   = dupDetail ? (dupDetail.db_owner_name || dupDetail.owner_name) : (r.owner_name || '');
            const dbId      = dupDetail ? dupDetail.id_passport_no : (r.id_passport_no || '');
            const dbDob     = dupDetail ? dupDetail.date_of_birth : (r.date_of_birth || '');
            const dbAcct    = dupDetail ? dupDetail.account_number : (r.account_number || '');
            const dbTx      = dupDetail ? dupDetail.last_transaction : (r.last_transaction || '');
            const dbAmt     = dupDetail ? dupDetail.due_amount : (r.due_amount || '');
            const dbComp    = dupDetail ? dupDetail.compilation_date : (r.compilation_date || '');
            const dbCreated = dupDetail
                ? (dupDetail.type === 'database'
                    ? (dupDetail.created_at || 'Previously Uploaded')
                    : 'Same File')
                : 'In-File Match';

            drawerHtml = `
            <tr style="background:#fff1f2;">
                <td colspan="9" style="padding:.65rem .85rem; border-bottom:2px solid #fca5a5;">
                    <div style="background:#fff; border:1px solid #fca5a5; border-radius:9px; padding:.65rem .85rem; box-shadow:0 2px 8px rgba(220,38,38,0.08);">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:.5rem; padding-bottom:.35rem; border-bottom:1px dashed #fca5a5;">
                            <div style="display:flex; align-items:center; gap:.6rem;">
                                ${matchedLabel}
                                <span style="font-size:.78rem; font-weight:700; color:#991b1b;">
                                    <i class="fa-solid fa-database"></i> Original Entry Stored in Database
                                </span>
                            </div>
                            <span style="font-size:.72rem; color:#64748b;"><i class="fa-solid fa-clock"></i> Uploaded: ${escHtml(dbCreated)}</span>
                        </div>
                        <div style="overflow-x:auto;">
                            <table style="width:100%; border-collapse:collapse; font-size:.78rem;">
                                <thead>
                                    <tr style="background:#fef2f2; color:#991b1b; font-weight:700; text-transform:uppercase; font-size:.65rem;">
                                        <th style="padding:.4rem .6rem; text-align:left; border:1px solid #fecaca;">Original Source</th>
                                        <th style="padding:.4rem .6rem; text-align:left; border:1px solid #fecaca;">Owner Name</th>
                                        <th style="padding:.4rem .6rem; text-align:left; border:1px solid #fecaca;">ID / Passport No</th>
                                        <th style="padding:.4rem .6rem; text-align:left; border:1px solid #fecaca;">Date of Birth</th>
                                        <th style="padding:.4rem .6rem; text-align:left; border:1px solid #fecaca;">Account Number</th>
                                        <th style="padding:.4rem .6rem; text-align:left; border:1px solid #fecaca;">Last Transaction</th>
                                        <th style="padding:.4rem .6rem; text-align:right; border:1px solid #fecaca;">Due Amount (KES)</th>
                                        <th style="padding:.4rem .6rem; text-align:left; border:1px solid #fecaca;">Compilation Date</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr style="background:#fff5f5; font-weight:600;">
                                        <td style="padding:.4rem .6rem; font-weight:800; color:#991b1b; border:1px solid #fecaca;">
                                            ${isDb ? `Original DB Record #${dupDetail.db_id}` : `In-File Match Row #${dupDetail ? (dupDetail.matched_with_row === rowNum ? dupDetail.row_num : dupDetail.matched_with_row) : ''}`}
                                        </td>
                                        <td style="padding:.4rem .6rem; border:1px solid #fecaca; color:#991b1b;">${escHtml(dbOwner)}</td>
                                        <td style="padding:.4rem .6rem; border:1px solid #fecaca; color:#991b1b;">${escHtml(dbId)}</td>
                                        <td style="padding:.4rem .6rem; border:1px solid #fecaca; color:#991b1b;">${escHtml(dbDob)}</td>
                                        <td style="padding:.4rem .6rem; border:1px solid #fecaca; color:#991b1b; font-weight:700;">${escHtml(dbAcct)}</td>
                                        <td style="padding:.4rem .6rem; border:1px solid #fecaca; color:#991b1b;">${escHtml(dbTx)}</td>
                                        <td style="padding:.4rem .6rem; text-align:right; border:1px solid #fecaca; color:#991b1b; font-weight:700;">${escHtml(dbAmt)}</td>
                                        <td style="padding:.4rem .6rem; border:1px solid #fecaca; color:#991b1b;">${escHtml(dbComp)}</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </td>
            </tr>`;
        }

        html += `
        <tr id="gr-${globalRowIdx}" style="${r.has_critical ? 'background:#fff5f5;' : pIdx%2===0?'':'background:#f8fafc;'}">
            <td style="padding:.45rem .4rem;text-align:center;font-weight:700;color:#94a3b8;font-size:.75rem;border-right:1px solid #f1f5f9;white-space:nowrap;">
                ${rowNum}
                ${isDup ? `
                    <button onclick="toggleDuplicateDrawer(${globalRowIdx})" class="btn-reset" style="margin-left:4px; padding:1px 5px; font-size:.7rem; color:#dc2626; border:1px solid #fca5a5; background:#fff; border-radius:4px; cursor:pointer; vertical-align:middle;" title="Click to show/hide original database match">
                        <i class="fa-solid ${isDrawerOpen ? 'fa-chevron-up' : 'fa-chevron-down'}"></i>
                    </button>
                ` : ''}
            </td>
            <td contenteditable="true" data-row="${globalRowIdx}" data-field="owner_name"       onblur="updateCell(${globalRowIdx},'owner_name',this.innerText)"       style="padding:.45rem .65rem;border-right:1px solid #f1f5f9;${cst('owner_name')}"       title="${ct('owner_name')}">${cellVal('owner_name', r.owner_name)}</td>
            <td contenteditable="true" data-row="${globalRowIdx}" data-field="id_passport_no"   onblur="updateCell(${globalRowIdx},'id_passport_no',this.innerText)"   style="padding:.45rem .65rem;border-right:1px solid #f1f5f9;${cst('id_passport_no')}"   title="${ct('id_passport_no')}">${cellVal('id_passport_no', r.id_passport_no)}</td>
            <td contenteditable="true" data-row="${globalRowIdx}" data-field="date_of_birth"    onblur="updateCell(${globalRowIdx},'date_of_birth',this.innerText)"    style="padding:.45rem .65rem;border-right:1px solid #f1f5f9;${cst('date_of_birth')}"    title="${ct('date_of_birth')}">${cellVal('date_of_birth', r.date_of_birth)}</td>
            <td contenteditable="true" data-row="${globalRowIdx}" data-field="account_number"   onblur="updateCell(${globalRowIdx},'account_number',this.innerText)"   style="padding:.45rem .65rem;font-weight:600;border-right:1px solid #f1f5f9;${cst('account_number')}"   title="${ct('account_number')}">${cellVal('account_number', r.account_number)}</td>
            <td contenteditable="true" data-row="${globalRowIdx}" data-field="last_transaction" onblur="updateCell(${globalRowIdx},'last_transaction',this.innerText)" style="padding:.45rem .65rem;border-right:1px solid #f1f5f9;${cst('last_transaction')}" title="${ct('last_transaction')}">${cellVal('last_transaction', r.last_transaction)}</td>
            <td contenteditable="true" data-row="${globalRowIdx}" data-field="due_amount"       onblur="updateCell(${globalRowIdx},'due_amount',this.innerText)"       style="padding:.45rem .65rem;text-align:right;font-weight:600;border-right:1px solid #f1f5f9;${cst('due_amount')}"       title="${ct('due_amount')}">${cellVal('due_amount', r.due_amount)}</td>
            <td contenteditable="true" data-row="${globalRowIdx}" data-field="compilation_date" onblur="updateCell(${globalRowIdx},'compilation_date',this.innerText)" style="padding:.45rem .65rem;border-right:1px solid #f1f5f9;${cst('compilation_date')}" title="${ct('compilation_date')}">${cellVal('compilation_date', r.compilation_date)}</td>
            <td style="padding:.45rem;text-align:center;white-space:nowrap;">
                ${isDup && dupDetail && dupDetail.type === 'in_file'
                    ? `<button onclick="keepOneDuplicateRow(${globalRowIdx})" class="btn-reset" style="padding:.22rem .55rem;font-size:.7rem;font-weight:700;color:#6d28d9;background:#ede9fe;border:1px solid #c4b5fd;border-radius:6px;cursor:pointer;" title="Keep this row and remove all duplicate copies">Keep 1</button>`
                    : `<button onclick="removeGridRow(${globalRowIdx})" class="btn-reset" style="padding:.2rem .45rem;font-size:.75rem;color:#ef4444;" title="Delete this record"><i class="fa-solid fa-trash-can"></i></button>`
                }
            </td>
        </tr>
        ${drawerHtml}`;
    }

    // Bottom virtual spacer
    if (bottomSpacerHeight > 0) {
        html += `<tr style="height:${bottomSpacerHeight}px;"><td colspan="9" style="padding:0;border:none;background:transparent;"></td></tr>`;
    }

    tbody.innerHTML = html;
}

function toggleDuplicateDrawer(origIdx) {
    if (expandedDrawers.has(origIdx)) {
        expandedDrawers.delete(origIdx);
    } else {
        expandedDrawers.add(origIdx);
    }
    renderVirtualRows(true);
}

/* ─── 6-Field Duplicate Key Generator (Identical to Server Engine) ── */
function make6FieldKey(r) {
    if (!r) return '';
    const normId   = String(r.id_passport_no || '').trim().toUpperCase();
    const normDob  = flexDate(r.date_of_birth);
    const normAcct = String(r.account_number || '').trim().toUpperCase().replace(/[\s\-_]/g, '');
    const normTx   = flexDate(r.last_transaction);
    const cleanAmt = String(r.due_amount || '').replace(/[^\d\.\-]/g, '');
    const normAmt  = (!isNaN(parseFloat(cleanAmt)) && isFinite(cleanAmt)) ? parseFloat(cleanAmt).toFixed(2) : String(r.due_amount || '').trim().toLowerCase();
    const normComp = flexDate(r.compilation_date);
    return `${normId}|${normDob}|${normAcct}|${normTx}|${normAmt}|${normComp}`;
}

/* ─── Optimistic Summary Recalculator ───────────────────── */
function updateOptimisticSummary() {
    let clean = 0, critical = 0, warnings = 0, dupCount = 0;
    for (let i = 0; i < gridRows.length; i++) {
        const r = gridRows[i];
        const rowNum = i + 1;
        const hasCritical = r.has_critical || (r.errors && Object.keys(r.errors).length > 0);
        const hasWarnings = r.warnings && Object.keys(r.warnings).length > 0;
        const hasDupErr   = r.errors && r.errors.account_number && r.errors.account_number.toLowerCase().includes('duplicate');
        const inDetails   = latestDuplicateDetails.some(d => d.row_num === rowNum || (d.type === 'in_file' && d.matched_with_row === rowNum));
        
        if (hasCritical) critical++;
        if (hasWarnings) warnings++;
        if (!hasCritical && !hasWarnings) clean++;
        if (hasDupErr || inDetails) dupCount++;
    }
    const totalEl = document.getElementById('summary-total');
    const cleanEl = document.getElementById('summary-clean');
    const critEl  = document.getElementById('summary-critical');
    const warnEl  = document.getElementById('summary-warnings');
    const dupEl   = document.getElementById('summary-duplicates');

    if (totalEl) totalEl.textContent = gridRows.length.toLocaleString();
    if (cleanEl) cleanEl.textContent = clean.toLocaleString();
    if (critEl)  critEl.textContent  = critical.toLocaleString();
    if (warnEl)  warnEl.textContent  = warnings.toLocaleString();
    if (dupEl)   dupEl.textContent   = dupCount.toLocaleString();
}

/* ─── Keep 1 In-File Duplicate (Real-Time Disappearance) ── */
async function keepOneDuplicateRow(keptIdx) {
    if (!gridRows[keptIdx]) return;
    pushStateToHistory();

    const keptRow = gridRows[keptIdx];
    const key = make6FieldKey(keptRow);

    // Find all other rows matching this exact 6-field key
    const toRemoveIndices = [];
    for (let i = 0; i < gridRows.length; i++) {
        if (i !== keptIdx && make6FieldKey(gridRows[i]) === key) {
            toRemoveIndices.push(i);
        }
    }

    if (toRemoveIndices.length === 0) {
        // If no matching rows found by key, treat kept row as unique and remove any duplicate label
        if (keptRow.errors && keptRow.errors.account_number && keptRow.errors.account_number.toLowerCase().includes('duplicate')) {
            delete keptRow.errors.account_number;
            keptRow.has_critical = Object.keys(keptRow.errors).length > 0;
            keptRow.is_clean     = !keptRow.has_critical && (!keptRow.warnings || Object.keys(keptRow.warnings).length === 0);
        }
        renderGridTable();
        return;
    }

    // Sort descending so splicing from highest index does not disturb lower indices
    toRemoveIndices.sort((a, b) => b - a);

    // Splice out all duplicate copies from gridRows
    toRemoveIndices.forEach(delIdx => {
        gridRows.splice(delIdx, 1);
    });

    // Compute new index for kept row after shifts
    const shiftCount = toRemoveIndices.filter(i => i < keptIdx).length;
    const newKeptIdx = keptIdx - shiftCount;
    const finalKeptRow = gridRows[newKeptIdx];

    // Clear duplicate error on the kept row because it is now unique in the file!
    if (finalKeptRow && finalKeptRow.errors && finalKeptRow.errors.account_number &&
        finalKeptRow.errors.account_number.toLowerCase().includes('duplicate')) {
        delete finalKeptRow.errors.account_number;
        finalKeptRow.has_critical = Object.keys(finalKeptRow.errors).length > 0;
        finalKeptRow.is_clean     = !finalKeptRow.has_critical && (!finalKeptRow.warnings || Object.keys(finalKeptRow.warnings).length === 0);
    }

    // Remove this key completely from latestDuplicateDetails
    latestDuplicateDetails = latestDuplicateDetails.filter(d => make6FieldKey(d) !== key);

    // Clear expanded drawer states
    expandedDrawers.clear();

    // 0ms REAL-TIME UI UPDATE: recalculate counters & re-render grid immediately
    updateOptimisticSummary();
    renderGridTable();

    // Persist batch deletions to server in background
    if (currentDraftId) {
        try {
            await fetch('ajax/sandbox_draft_api.php?action=delete_rows', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ draft_id: currentDraftId, row_indices: toRemoveIndices })
            });
        } catch (e) {
            saveInitialDraft();
            return;
        }
    }

    // Background server validation to synchronize full database checks
    validateGrid();
}

/* ─── Cell update ──────────────────────────────────────── */
async function updateCell(rowIdx, field, val) {
    if (!gridRows[rowIdx]) return;
    const clean = txt(val).replace(/\(missing\)/g, '');
    if (gridRows[rowIdx][field] === clean) return;

    pushStateToHistory();

    gridRows[rowIdx][field] = clean;
    validateGrid();

    if (currentDraftId) {
        await fetch('ajax/sandbox_draft_api.php?action=patch_cell', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ draft_id: currentDraftId, row_index: rowIdx, field, val: clean })
        });
    }
}

/* ─── Real-Time Single Row Deletion ─────────────────────── */
async function removeGridRow(idx) {
    if (!gridRows[idx]) return;
    pushStateToHistory();

    const targetRow = gridRows[idx];
    const key = make6FieldKey(targetRow);

    // Remove row from memory immediately
    gridRows.splice(idx, 1);

    // If this row was an in-file duplicate, check if only 1 matching row remains
    const remainingMatches = [];
    for (let i = 0; i < gridRows.length; i++) {
        if (make6FieldKey(gridRows[i]) === key) {
            remainingMatches.push(i);
        }
    }
    // If exactly 1 row remains with this key, it is no longer duplicate! Clear its duplicate error!
    if (remainingMatches.length === 1) {
        const remainingRow = gridRows[remainingMatches[0]];
        if (remainingRow && remainingRow.errors && remainingRow.errors.account_number &&
            remainingRow.errors.account_number.toLowerCase().includes('duplicate')) {
            delete remainingRow.errors.account_number;
            remainingRow.has_critical = Object.keys(remainingRow.errors).length > 0;
            remainingRow.is_clean     = !remainingRow.has_critical && (!remainingRow.warnings || Object.keys(remainingRow.warnings).length === 0);
        }
        latestDuplicateDetails = latestDuplicateDetails.filter(d => make6FieldKey(d) !== key);
    } else {
        // Re-index remaining duplicate details
        latestDuplicateDetails = latestDuplicateDetails
            .filter(d => d.row_num !== idx + 1 && !(d.type === 'in_file' && d.matched_with_row === idx + 1))
            .map(d => ({
                ...d,
                row_num:          d.row_num          > idx + 1 ? d.row_num          - 1 : d.row_num,
                matched_with_row: d.matched_with_row > idx + 1 ? d.matched_with_row - 1 : d.matched_with_row
            }));
    }

    // Re-map expanded drawers
    const newExpanded = new Set();
    expandedDrawers.forEach(i => { if (i !== idx) newExpanded.add(i > idx ? i - 1 : i); });
    expandedDrawers = newExpanded;

    // 0ms REAL-TIME UI UPDATE: row disappears on screen immediately!
    updateOptimisticSummary();
    renderGridTable();

    // Persist deletion immediately via API (background sync)
    if (currentDraftId) {
        try {
            await fetch('ajax/sandbox_draft_api.php?action=delete_row', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ draft_id: currentDraftId, row_index: idx })
            });
        } catch (e) {
            saveInitialDraft();
            return;
        }
    }

    validateGrid();
}


/* ─── Date fix ─────────────────────────────────────────── */
function autoFixAllCompilationDates() {
    let n = 0;
    pushStateToHistory();
    gridRows.forEach(r => { const p = flexDate(r.compilation_date); if (p && p !== r.compilation_date) { r.compilation_date = p; n++; } });
    validateGrid();
    saveInitialDraft();
    alert(`Fixed ${n.toLocaleString()} compilation date(s) to YYYY-MM-DD.`);
}

function flexDate(s) {
    if (!s) return '';
    s = String(s).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const m = s.match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{4})/);
    if (m) return `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
    const d = new Date(s); return isNaN(d) ? s : d.toISOString().split('T')[0];
}

/* ─── Save full draft ──────────────────────────────────── */
async function saveInitialDraft() {
    if (!gridRows.length) return;
    try {
        const res  = await fetch('ajax/sandbox_draft_api.php?action=save', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                draft_id:   currentDraftId,
                draft_name: currentDraftName || 'Working Draft',
                rows:       gridRows
            })
        });
        if (!res.ok) return;
        const data = await res.json();
        if (data.status === 'success') {
            currentDraftId = data.draft_id;
            document.getElementById('workspace-meta').textContent = `Auto-saved · ${data.updated_at}`;
        }
    } catch (err) {}
}

/* ─── Summary cards ────────────────────────────────────── */
function updateSummaryCards(s) {
    document.getElementById('summary-total').textContent      = (s.total || 0).toLocaleString();
    document.getElementById('summary-clean').textContent      = (s.clean || 0).toLocaleString();
    document.getElementById('summary-critical').textContent   = (s.critical || 0).toLocaleString();
    document.getElementById('summary-warnings').textContent   = (s.warnings || 0).toLocaleString();
    
    const dups = (s.in_file_duplicates||0) + (s.db_duplicates||0);
    document.getElementById('summary-duplicates').textContent = dups.toLocaleString();

    const btn  = document.getElementById('btn-submit-locked');
    const icon = document.getElementById('btn-submit-icon');
    const lbl  = document.getElementById('btn-submit-text');
    // TEMPORARY OVERRIDE: Allow upload & lock even with errors for historical data import
    btn.disabled = false;
    btn.style.opacity    = '1';
    btn.style.cursor     = 'pointer';
    btn.style.background = '#16a34a';
    btn.style.color      = '#fff';
    icon.className       = 'fa-solid fa-cloud-arrow-up';
    lbl.textContent      = (s.critical > 0)
        ? `Upload & Lock File (${s.critical.toLocaleString()} Errors Bypassed)`
        : 'Upload & Lock File';
}

/* ─── Export ───────────────────────────────────────────── */
function exportDraftExcel() {
    if (!gridRows.length) return;
    const ws = XLSX.utils.json_to_sheet(gridRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Draft');
    XLSX.writeFile(wb, `UFAA_Draft_${new Date().toISOString().split('T')[0]}.xlsx`);
}

/* ─── Export Current Filter ─────────────────────────────── */
function exportFilteredExcel() {
    if (!filteredRowIndices.length) {
        alert('No rows match the current filter to export.');
        return;
    }

    const filterLabel = {
        all:        'All',
        clean:      'Clean',
        critical:   'Errors',
        warnings:   'Warnings',
        duplicates: 'Duplicates'
    }[currentCardFilter] || currentCardFilter;

    const isDupExport = (currentCardFilter === 'duplicates');
    const exportRows  = [];
    const dateStr     = new Date().toISOString().split('T')[0];

    filteredRowIndices.forEach(globalIdx => {
        const r      = gridRows[globalIdx];
        const rowNum = globalIdx + 1;
        if (!r) return;

        const baseRow = {
            'Row #':             rowNum,
            'Owner Name':        r.owner_name        || '',
            'ID / Passport No':  r.id_passport_no    || '',
            'Date of Birth':     r.date_of_birth     || '',
            'Account Number':    r.account_number    || '',
            'Last Transaction':  r.last_transaction  || '',
            'Due Amount (KES)':  r.due_amount        || '',
            'Compilation Date':  r.compilation_date  || ''
        };

        // For errors and warnings exports, include the issue description
        if (currentCardFilter === 'critical' || currentCardFilter === 'warnings') {
            const issues = [];
            const src    = currentCardFilter === 'critical' ? (r.errors || {}) : (r.warnings || {});
            Object.entries(src).forEach(([field, msg]) => issues.push(`[${field}] ${msg}`));
            baseRow['Issues'] = issues.join(' | ');
        }

        if (isDupExport) {
            const dupDetail = latestDuplicateDetails.find(
                d => d.row_num === rowNum || (d.type === 'in_file' && d.matched_with_row === rowNum)
            );

            if (dupDetail) {
                const isDb = dupDetail.type === 'database';
                baseRow['Duplicate Type']         = isDb ? 'DB Duplicate' : 'In-File Duplicate';
                baseRow['Matched With']            = isDb
                    ? `DB Record #${dupDetail.db_id} (${dupDetail.db_owner_name || ''})`
                    : `Row #${dupDetail.matched_with_row === rowNum ? dupDetail.row_num : dupDetail.matched_with_row}`;

                // Original / matched record columns
                baseRow['[ORIGINAL] Owner Name']       = dupDetail.db_owner_name  || dupDetail.owner_name       || '';
                baseRow['[ORIGINAL] ID / Passport No'] = dupDetail.id_passport_no  || '';
                baseRow['[ORIGINAL] Date of Birth']    = dupDetail.date_of_birth   || '';
                baseRow['[ORIGINAL] Account Number']   = dupDetail.account_number  || '';
                baseRow['[ORIGINAL] Last Transaction'] = dupDetail.last_transaction || '';
                baseRow['[ORIGINAL] Due Amount (KES)'] = dupDetail.due_amount      || '';
                baseRow['[ORIGINAL] Compilation Date'] = dupDetail.compilation_date || '';
                baseRow['[ORIGINAL] Uploaded / Source']= isDb
                    ? (dupDetail.created_at || 'Previously Uploaded')
                    : 'Same File';
            } else {
                // Flagged as dup by error message but no detail (edge case)
                baseRow['Duplicate Type'] = 'Duplicate';
                baseRow['Matched With']   = r.errors?.account_number || '';
            }
        }

        exportRows.push(baseRow);
    });

    if (!exportRows.length) {
        alert('No data to export for the current filter.');
        return;
    }

    const ws = XLSX.utils.json_to_sheet(exportRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, filterLabel);
    XLSX.writeFile(wb, `UFAA_${filterLabel}_${dateStr}.xlsx`);
}

/* ─── Commit ───────────────────────────────────────────── */
async function commitSanitizedUpload() {

    const btn = document.getElementById('btn-submit-locked');
    btn.disabled = true;
    btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving to database…';
    try {
        const res = await fetch('ajax/commit_pre_submission.php', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ draft_id: currentDraftId })
        });
        const text = await res.text();
        let data;
        try {
            data = JSON.parse(text);
        } catch (jsonErr) {
            throw new Error('Server returned invalid response: ' + (text ? text.substring(0, 300) : 'Empty server response'));
        }
        if (data.status !== 'success') throw new Error(data.message || 'Submission failed');

        document.getElementById('success-modal-msg').textContent = data.message || 'Submission completed.';

        const dlBtn = document.getElementById('btn-modal-download');
        if (data.file_url && data.file_url !== 'undefined' && data.file_url !== 'null') {
            dlBtn.style.display  = 'inline-flex';
            dlBtn.href           = data.file_url;
            dlBtn.download       = data.file_name || 'UFAA_Submission.xlsx';
        } else {
            // Hide the download button if file generation was bypassed or failed
            dlBtn.style.display  = 'none';
        }

        document.getElementById('commit-success-modal').style.display = 'flex';
    } catch (err) { alert('Error: ' + err.message); }
    finally {
        applyOwnerPermissions();
    }
}

function triggerModalDownloadAndRedirect() {
    const btn = document.getElementById('btn-modal-download');
    if (btn) {
        btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Downloading…';
        btn.style.opacity = '0.85';
        btn.style.pointerEvents = 'none';
    }

    const url = btn ? btn.getAttribute('href') : '';
    if (url && url !== '#' && url !== 'undefined' && !url.includes('undefined')) {
        const fn  = btn.getAttribute('download') || 'UFAA_Submission.xlsx';
        const a   = document.createElement('a');
        a.href    = url;
        a.download= fn;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    setTimeout(() => {
        window.location.href = 'data_preparation.php';
    }, 1500);
}

function closeSuccessModal() {
    document.getElementById('commit-success-modal').style.display='none';
    window.location.href='data_preparation.php';
}

/* ─── Utilities ────────────────────────────────────────── */
function txt(v)          { return String(v ?? '').trim(); }
function fmtDate(v)      { if (!v) return ''; if (v instanceof Date) return v.toISOString().split('T')[0]; return txt(v); }
function cleanRawRows(rows) {
    return rows.map(r => ({
        owner_name: txt(r.owner_name), id_passport_no: txt(r.id_passport_no),
        date_of_birth: txt(r.date_of_birth), account_number: txt(r.account_number),
        last_transaction: txt(r.last_transaction), due_amount: txt(r.due_amount),
        compilation_date: txt(r.compilation_date)
    }));
}
function escHtml(s) { const d=document.createElement('div'); d.textContent=s||''; return d.innerHTML; }
function escAttr(s) { return escHtml(s).replace(/"/g,'&quot;'); }
</script>

<?php require_once 'includes/layout_footer.php'; ?>
