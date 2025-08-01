Option Explicit

Dim tblName, tbl, col
Dim output, semantics
Dim foundTable, foundVarchar
Dim cleanLength

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
            If InStr(UCase(col.DataType), "VARCHAR2") > 0 Then
                foundVarchar = True

                On Error Resume Next
                semantics = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then
                    semantics = "(not available)"
                    Err.Clear
                End If
                On Error GoTo 0

                ' Build proper suggestion
                cleanLength = col.Length
                output = output & "  Column: " & col.Code & vbCrLf
                output = output & "    DataType: " & col.DataType & vbCrLf
                output = output & "    Length: " & cleanLength & vbCrLf
                output = output & "    Semantics: " & semantics & vbCrLf

                If UCase(semantics) <> "CHAR" Then
                    output = output & "    ❗ Suggestion: Change to VARCHAR2(" & cleanLength & " CHAR) semantics" & vbCrLf
                Else
                    output = output & "    ✅ OK - CHAR semantics in place" & vbCrLf
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


====================

Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semantics, suggestion, cleanLength, fullSuggestion
Dim dataTypeUpper

' Set output CSV path (to Desktop)
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

' Write CSV header
file.WriteLine "Table,Column,DataType,Length,Semantics,Suggestion"

' Loop through tables
For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            dataTypeUpper = UCase(col.DataType)
            If InStr(dataTypeUpper, "VARCHAR2") > 0 Then
                On Error Resume Next
                semantics = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then
                    semantics = "(not available)"
                    Err.Clear
                End If
                On Error GoTo 0

                ' Build suggestion
                If UCase(semantics) <> "CHAR" Then
                    ' Extract numeric part of VARCHAR2(n) from DataType or use col.Length
                    cleanLength = col.Length
                    fullSuggestion = "Change to VARCHAR2(" & cleanLength & " CHAR) semantics"
                Else
                    fullSuggestion = "OK"
                End If

                ' Write row
                file.WriteLine """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & col.Length & """,""" & semantics & """,""" & fullSuggestion & """"
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ CSV report saved to Desktop as 'varchar2_semantics_report.csv'", vbInformation, "VARCHAR2 Semantics Audit"
