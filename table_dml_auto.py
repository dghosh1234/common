reat — here's a PowerDesigner VBScript "dry run" version of the previous script.
This version will:
* Scan all tables and columns in your Physical Data Model (PDM)
* Detect columns of type VARCHAR2 that are:
    * Using BYTE semantics
    * Or default semantics (neither CHAR nor BYTE explicitly set)
* List those columns in a pop-up message box
* Not change anything — it's safe to run

PowerDesigner VBScript: Preview (Dry Run)
Paste and run this in:
Tools → Execute Commands → Edit/Run Script





Option Explicit

Dim tbl, col
Dim report
Dim affectedCount
report = "Columns that would be updated to VARCHAR2(n CHAR):" & vbCrLf
affectedCount = 0

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            ' Check for VARCHAR2 data type
            If UCase(col.DataType) = "VARCHAR2" Then
                ' Check if semantics are not CHAR
                If col.LengthSemantics <> "CHAR" Then
                    report = report & "- Table: " & tbl.Name & ", Column: " & col.Name & ", Length: " & col.Length & vbCrLf
                    affectedCount = affectedCount + 1
                End If
            End If
        Next
    End If
Next

If affectedCount = 0 Then
    MsgBox "No columns need to be updated. All VARCHAR2 columns already use CHAR semantics.", vbInformation, "Dry Run Complete"
Else
    MsgBox report & vbCrLf & "Total: " & affectedCount & " column(s) would be updated.", vbInformation, "Dry Run Complete"
End If


You want a PowerDesigner VBScript that:
* Finds all columns in your Physical Data Model (PDM) where:
    * DataType = VARCHAR2
    * Length is defined like VARCHAR2(10) (implicitly BYTE)
* Then modifies them to:
    * VARCHAR2(10 CHAR) (i.e., adds CHAR semantics explicitly)
* Does not change the data type or length value — only changes BYTE → CHAR or adds CHAR if not set.

✅ Working PowerDesigner VBScript
Paste and run the following in:
Tools → Execute Commands → Edit/Run Script

Option Explicit

Dim tbl, col
Dim updatedCount
updatedCount = 0

' Loop through all tables in the model
For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            ' Only target columns of type VARCHAR2
            If UCase(col.DataType) = "VARCHAR2" Then
                ' Only update if not already using CHAR semantics
                If col.LengthSemantics <> "CHAR" Then
                    col.LengthSemantics = "CHAR"
                    updatedCount = updatedCount + 1
                End If
            End If
        Next
    End If
Next

MsgBox updatedCount & " VARCHAR2 columns were converted to use CHAR semantics.", vbInformation, "Conversion Complete"



****************************************************************
If want to see all tables are affecting having varcha2 column will change it to varcha2(10 char)

If you're happy with the results, you can run the actual updating script (from my previous reply) to apply the CHARsemantics to those columns.
Would you like:
* The script to also output to a file?
* Or skip specific tables (e.g., system, audit tables)?
Let me know how you’d like to enhance it.

You said:
want to implement for a particular table

ChatGPT said:
Perfect — you want to run the PowerDesigner VBScript dry run for a specific table only (rather than scanning all tables in the model).




✅ Updated Script: Dry Run for One Table
This version:
* Asks you to enter the table name
* Checks only that table
* Reports all VARCHAR2 columns not using CHAR semantics
* Makes no changes




Option Explicit

Dim tbl, col
Dim targetTableName
Dim found, affectedCount
Dim report

' Prompt user for table name
targetTableName = InputBox("Enter the table name to check (case-sensitive):", "Target Table")

If targetTableName = "" Then
    MsgBox "No table name entered. Script cancelled.", vbExclamation, "Cancelled"
    Exit Sub
End If

report = "Columns in table '" & targetTableName & "' that would be updated to VARCHAR2(n CHAR):" & vbCrLf
affectedCount = 0
found = False

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut And tbl.Name = targetTableName Then
        found = True
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                If col.LengthSemantics <> "CHAR" Then
                    report = report & "- Column: " & col.Name & ", Length: " & col.Length & vbCrLf
                    affectedCount = affectedCount + 1
                End If
            End If
        Next
        Exit For
    End If
Next

If Not found Then
    MsgBox "Table '" & targetTableName & "' not found in the model.", vbExclamation, "Table Not Found"
ElseIf affectedCount = 0 Then
    MsgBox "All VARCHAR2 columns in table '" & targetTableName & "' already use CHAR semantics.", vbInformation, "Dry Run Complete"
Else
    MsgBox report & vbCrLf & "Total: " & affectedCount & " column(s) would be updated.", vbInformation, "Dry Run Complete"
End If



Example Flow:
1. Script prompts: ➤ Enter the table name to check (case-sensitive): → You enter: CUSTOMERS
2. Output shows:

Columns in table 'CUSTOMERS' that would be updated to VARCHAR2(n CHAR):
- Column: NAME, Length: 100
- Column: EMAIL, Length: 255

Total: 2 column(s) would be updated.


******************************************************************************************************************
Now apply actual update to a particular table

Great! Here's the PowerDesigner VBScript that:
* Prompts you for a specific table name
* Searches for that table in the Physical Data Model
* Updates only VARCHAR2 columns in that table that are:
    * Not already using CHAR semantics
* Sets them to use CHAR semantics (i.e., VARCHAR2(n CHAR))


✅ Script: Actual Update for One Table (VARCHAR2 → CHAR)
Run it from Tools → Execute Commands → Edit/Run Script



Option Explicit

Dim tbl, col
Dim targetTableName
Dim found, updatedCount

' Prompt user for table name
targetTableName = InputBox("Enter the table name to update (case-sensitive):", "Target Table")

If targetTableName = "" Then
    MsgBox "No table name entered. Script cancelled.", vbExclamation, "Cancelled"
    Exit Sub
End If

updatedCount = 0
found = False

' Search for the table
For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut And tbl.Name = targetTableName Then
        found = True
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                If col.LengthSemantics <> "CHAR" Then
                    col.LengthSemantics = "CHAR"
                    updatedCount = updatedCount + 1
                End If
            End If
        Next
        Exit For
    End If
Next

' Show results
If Not found Then
    MsgBox "Table '" & targetTableName & "' not found in the model.", vbExclamation, "Table Not Found"
ElseIf updatedCount = 0 Then
    MsgBox "No VARCHAR2 columns needed updating in table '" & targetTableName & "'.", vbInformation, "No Changes"
Else
    MsgBox updatedCount & " VARCHAR2 column(s) in table '" & targetTableName & "' updated to use CHAR semantics.", vbInformation, "Update Complete"
End If


*******************************************************************************************************

Example Run:
1. Script asks: ➤ "Enter the table name to update:" → You enter: CUSTOMERS
2. Output: ✅ "3 VARCHAR2 column(s) in table 'CUSTOMERS' updated to use CHAR semantics."

✅ What Happens
Column	Before	After
NAME	VARCHAR2(100)	VARCHAR2(100 CHAR)
EMAIL	VARCHAR2(255 BYTE)	VARCHAR2(255 CHAR)
No changes are made to other tables.

🚀 Want More?
Let me know if you want versions that:
* Work on a list of tables
* Log changes to a text file
* Only update columns longer than N characters
* Exclude specific columns (e.g., ID, GUID)
