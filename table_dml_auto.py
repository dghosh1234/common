Option Explicit

Dim tblName, tbl, col
Dim output, semantics
Dim foundTable, foundVarchar

tblName = InputBox("Enter the table code (not name) to inspect:", "Table Selector")
If tblName = "" Then
    MsgBox "No table name provided. Exiting.", vbExclamation, "Canceled"
    WScript.Quit
End If

foundTable = False
foundVarchar = False
output = "Inspecting VARCHAR2 columns in table: " & tblName & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut And UCase(tbl.Code) = UCase(tblName) Then
        foundTable = True
        For Each col In tbl.Columns
            ' Use InStr to match anything that contains VARCHAR2
            If InStr(UCase(col.DataType), "VARCHAR2") > 0 Then
                foundVarchar = True

                On Error Resume Next
                semantics = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then
                    semantics = "(not available)"
                    Err.Clear
                End If
                On Error GoTo 0
                
                output = output & "  Column: " & col.Code & vbCrLf
                output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
                output = output & "    Length: " & col.Length & vbCrLf
                output = output & "    Semantics: " & semantics & vbCrLf

                If UCase(semantics) <> "CHAR" Then
                    output = output & "    --> Needs update to CHAR semantics" & vbCrLf
                End If
                output = output & vbCrLf
            End If
        Next
        Exit For
    End If
Next

If Not foundTable Then
    MsgBox "❌ Table '" & tblName & "' not found in the model.", vbCritical, "Error"
ElseIf Not foundVarchar Then
    MsgBox "✅ Table '" & tblName & "' found, but no VARCHAR2 columns exist.", vbInformation, "No Columns"
Else
    MsgBox output, vbOKOnly, "VARCHAR2 Semantics Check"
End If


Option Explicit

Dim tblName, tbl, col
Dim output, semantics
Dim found

tblName = InputBox("Enter the table code (not name) to inspect:", "Table Selector")
If tblName = "" Then
    MsgBox "No table name provided. Exiting.", vbExclamation, "Canceled"
    WScript.Quit
End If

found = False
output = "Inspecting VARCHAR2 columns in table: " & tblName & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut And UCase(tbl.Code) = UCase(tblName) Then
        found = True
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                semantics = col.GetExtendedAttribute("LengthSemantics")
                
                output = output & "  Column: " & col.Code & vbCrLf
                output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
                output = output & "    Length: " & col.Length & vbCrLf
                output = output & "    Semantics: " & semantics & vbCrLf

                If semantics <> "CHAR" Then
                    output = output & "    --> Needs update to CHAR semantics" & vbCrLf
                End If
                output = output & vbCrLf
            End If
        Next
        Exit For
    End If
Next

If Not found Then
    MsgBox "Table '" & tblName & "' not found in the model.", vbCritical, "Error"
Else
    MsgBox output, vbOKOnly, "VARCHAR2 Semantics Check"
End If





Option Explicit

Dim tbl, col
Dim output

output = "Inspecting all columns in the Physical Model:" & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        output = output & "Table: " & tbl.Code & vbCrLf
        For Each col In tbl.Columns
            output = output & "  Column: " & col.Code & vbCrLf
            output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
            output = output & "    Length: " & col.Length & vbCrLf
            
            ' Check if LengthSemantics is available and print its value
            On Error Resume Next
            Dim colSemantics
            colSemantics = col.LengthSemantics
            If Err.Number <> 0 Then
                output = output & "    LengthSemantics: [N/A]" & vbCrLf
                Err.Clear
            Else
                output = output & "    LengthSemantics: '" & col.LengthSemantics & "'" & vbCrLf
            End If
            On Error GoTo 0
        Next
        output = output & vbCrLf
    End If
Next

' Show the detailed debug output
MsgBox output, vbOKOnly, "DEBUG: Column DataTypes and Semantics"


Option Explicit

Dim tbl, col
Dim output

output = "Inspecting all VARCHAR2 columns in the Physical Model:" & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        output = output & "Table: " & tbl.Code & vbCrLf
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                output = output & "  Column: " & col.Code & vbCrLf
                output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
                output = output & "    Length: " & col.Length & vbCrLf
            End If
        Next
    End If
Next

MsgBox output, vbOKOnly, "DEBUG: VARCHAR2 Columns"


Option Explicit

Dim tbl, col
Dim output

output = "Inspecting all VARCHAR2 columns in the Physical Model:" & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        output = output & "Table: " & tbl.Code & vbCrLf
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                output = output & "  Column: " & col.Code & vbCrLf
                output = output & "    DataType: '" & col.DataType & "'" & vbCrLf
                output = output & "    Length: " & col.Length & vbCrLf
                
                ' If Length is in bytes and not set to CHAR semantics, mark as needing update
                If col.Length > 0 Then
                    output = output & "    This column uses BYTE length semantics and needs update to CHAR semantics" & vbCrLf
                End If
            End If
        Next
        output = output & vbCrLf
    End If
Next

' Show the detailed debug output
MsgBox output, vbOKOnly, "DEBUG: VARCHAR2 Columns"
