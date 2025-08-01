Option Explicit

Dim mdl
Set mdl = ActiveModel

If mdl Is Nothing Then
    MsgBox "❌ No active model detected. Please make sure a PDM is open and selected.", vbCritical, "Script Error"
    WScript.Quit
End If

MsgBox "✅ Active model name: " & mdl.Name & vbCrLf & _
       "Model Type: " & mdl.Type & vbCrLf & _
       "Number of Tables: " & mdl.Tables.Count, vbInformation, "Model Info"

Dim tbl, col
For Each tbl In mdl.Tables
    MsgBox "Table: " & tbl.Code & ", Columns: " & tbl.Columns.Count
    For Each col In tbl.Columns
        MsgBox "  → Column: " & col.Code & ", DataType: " & col.DataType
    Next
Next



Option Explicit

Dim fso, file, filePath
Dim tbl, col
Dim dataTypeRaw, semantics, cleanLength, suggestion, outputLine

' 🔍 Ensure a model is loaded
If ActiveModel Is Nothing Then
    MsgBox "❌ No active model found. Please open a model and try again.", vbCritical, "Model Not Found"
    WScript.Quit
End If

' File path on Desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

' Loop through all tables and columns
For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            dataTypeRaw = UCase(col.DataType)

            If InStr(dataTypeRaw, "VARCHAR2") > 0 Then
                cleanLength = col.Length
                semantics = ""

                ' Check for CHAR/BYTE in DataType string first
                If InStr(dataTypeRaw, "CHAR") > 0 Then
                    semantics = "CHAR"
                ElseIf InStr(dataTypeRaw, "BYTE") > 0 Then
                    semantics = "BYTE"
                Else
                    ' Fallback: try to read LengthSemantics extended attribute
                    On Error Resume Next
                    semantics = col.GetExtendedAttribute("LengthSemantics")
                    If Err.Number <> 0 Then semantics = "" : Err.Clear
                    On Error GoTo 0
                    semantics = UCase(Trim(semantics))
                    If semantics = "" Then semantics = "BYTE"
                End If

                ' Only flag if semantics is not CHAR
                If semantics <> "CHAR" Then
                    suggestion = "Change to VARCHAR2(" & cleanLength & " CHAR)"
                    outputLine = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & cleanLength & """,""" & semantics & """,""" & suggestion & """"
                    file.WriteLine outputLine
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ Report generated successfully at: " & filePath, vbInformation, "Varchar2 Semantics Check"
